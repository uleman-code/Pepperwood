'''Main module for the SensorDataIngest package for Pepperwood.'''

import logging

from dash_extensions.enrich import DashBlueprint, DashProxy, ServersideOutputTransform, TriggerTransform
from dash                   import _dash_renderer

import dash_mantine_components as dmc
import nestedtext              as nt

from typing            import Annotated, Any
from enum              import StrEnum
from pathlib           import Path
from pydantic          import BaseModel, BeforeValidator, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

import callbacks

# Configuration settings and validation
def normalize_config_key(key: str, parent_keys: list[str]) -> str:
    '''Make configuration keys case-insensitive and connect words by underscore.'''

    return '_'.join(key.lower().split())

def to_upper(input: Any) -> str:
    '''Expect a string, or at least something that has a string representation; convert to uppercase.'''

    return str(input).upper()

def prepare_logging_level(input: Any) -> int:
    '''Accept a case-insensitive logging-level string or an integer between 0 and 50; return the (equivalent) integer.
    
    Strings should be from the familiar logging levels: DEBUG, INFO, WARNING, ERROR (NOTSET and CRITICAL also exist).
    Integers are truncated to the interval [0, 50] (equivalent to [NOTSET, CRITICAL]).

    Raises:
        ValueError  if input is neither an integer nor a valid logging-level string
    '''

    try:
        # Keep it within the 0 - 50 range.
        return min(max(int(input), logging.getLevelNamesMapping()['NOTSET']), logging.getLevelNamesMapping()['CRITICAL'])
    except ValueError:
        pass                # Not an integer; try string next

    try:
        if isinstance(input, str):
            return logging.getLevelNamesMapping()[input.upper()]        # case-insensitive
        else:
            raise ValueError(f'"{input}" cannot be a valid logging level. It needs to be a string or an integer.')
    except KeyError:
        raise ValueError(f'"{input}" is not a valid logging level. Use DEBUG, INFO, WARNING, or ERROR')

class ApplicationCfg(BaseModel):
    '''General application-wide settings. Should not be sensor- or datalogger-specific.'''

    config_version:             str
    debug:                      bool         = False
    console_logging_level:      Annotated[int, BeforeValidator(prepare_logging_level)] = logging.getLevelNamesMapping()['INFO']
    file_logging_level:         Annotated[int, BeforeValidator(prepare_logging_level)] = logging.getLevelNamesMapping()['DEBUG']
    logging_directory:          Path         = Path('./logs')
    datalogger_file_extensions: list[str]    = ['.dat', '.csv']     # For now, keep these input settings here, too
    excel_file_extensions:      list[str]    = ['.xlsx', '.xls']

class UiTextCfg(BaseModel):
    '''All text that shows up in the browser (except the application title).'''

    load_data:    str = 'Load Data'
    drag_n_drop:  str = 'Drag and drop, or'
    select_files: str = 'Select File(s)'
class OutputCfg(BaseModel):
    '''Output settings. Don't know if there will ever be anything besides worksheet names.'''

    worksheet_names: dict[str, str] = dict(data='Data', meta='Columns', site='Meta', notes='Notes')
    data_na_representation: str     = '#N/A'

class MetadataCfg(BaseModel):
    '''Metadata settings: not about the sensor data but only the standard index columns and what's in the other worksheets.'''

    timestamp_column:       str
    sequence_number_column: str
    sampling_interval:      str
    description_columns:    list[str]
    site_columns:           list[str]
    notes_columns:          list[str]

class Config(BaseSettings):
    '''All configuration settings, except config_file, normally come from a file but may be overridden using commandline arguments or equivalent environment variables.
    
    For any commandline argument, for example "--application.debug true", the equivalent environment expression is "INGEST_APPLICATION.DEBUG=true" (note the prefix "INGEST_").
    The setting names, whether on the commandline or as environment variables, are not case-sensitive. but the values may well be,
    depending on the specific setting and on the environment. For example, a file path is case-sensitive in MacOS but not in Windows.'''

    # BaseSettings (as opposed to BaseModel) looks for environment variables; look for command-line arguments as well.
    model_config: SettingsConfigDict    = SettingsConfigDict(cli_parse_args=True, env_prefix='ingest_') # pyright: ignore[reportIncompatibleVariableOverride]
    
    config_file:  Path                  = Path('./ingest.cfg')     # Any non-default value must come from the environment or the command line
    application:  ApplicationCfg | None = None
    output:       OutputCfg      | None = None
    metadata:     MetadataCfg    | None = None

# Declare as a global: should be accessible from all modules and functions.
# config: dict[str, Any] = {}

def main() -> None:
    # Get the configuration from a file in NestedText format. The default path may be overridden in the environment or on the command line.
    config_file: Path = Config(application=None, output=None, metadata=None).config_file # pyright: ignore[reportCallIssue]
    try:
        keymap = {}
        config_raw: dict[str, Any] = nt.load(config_file, keymap=keymap, normalize_key=normalize_config_key) # type: ignore
        config:     Config         = Config.model_validate(config_raw)
    except nt.NestedTextError as e:
        e.terminate()

    # Set up logging.
    # If logging_dir does not exist, create it.
    # The current setup errors out if logging_dir exists but is not a directory (because then creating the log file fails).
    logging_dir: Path = config.application.logging_directory # pyright: ignore[reportOptionalMemberAccess, reportPossiblyUnboundVariable]

    if not logging_dir.exists():
        logging_dir.mkdir()
        warn_logging_dir_created = True
    else:
        warn_logging_dir_created = False

    module_name = Path(__file__).stem

    # General-purpose logger
    logger           = logging.getLogger(module_name.capitalize())
    file_handler     = logging.FileHandler(logging_dir / (module_name + '.log'))
    stream_handler   = logging.StreamHandler()
    file_formatter   = logging.Formatter('{asctime}|{levelname:5s}|{module:9s}|{funcName:28s}: {message}', style='{', datefmt='%Y-%m-%d %H:%M:%S')
    stream_formatter = logging.Formatter('{levelname}|{name}: {message}', style='{')

    file_handler.setLevel(config.application.file_logging_level) # pyright: ignore[reportOptionalMemberAccess, reportPossiblyUnboundVariable]
    file_handler.setFormatter(file_formatter)
    stream_handler.setLevel(config.application.console_logging_level) # pyright: ignore[reportOptionalMemberAccess, reportPossiblyUnboundVariable]
    stream_handler.setFormatter(stream_formatter)

    logger.setLevel(config.application.file_logging_level) # pyright: ignore[reportOptionalMemberAccess, reportPossiblyUnboundVariable]
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Function entry/exit logger: substitute a custom name (the name of the wrapped function) for funcname.
    ee_logger    = logging.getLogger(module_name)
    ee_handler   = logging.FileHandler(logging_dir / (module_name + '.log'))
    ee_formatter = logging.Formatter('{asctime}|{levelname:5s}|{module:9s}|{fname:28s}: {message}', style='{', datefmt='%Y-%m-%d %H:%M:%S')

    ee_handler.setLevel(config.application.file_logging_level) # pyright: ignore[reportOptionalMemberAccess, reportPossiblyUnboundVariable]
    ee_handler.setFormatter(ee_formatter)
    ee_logger.setLevel(config.application.file_logging_level) # pyright: ignore[reportOptionalMemberAccess, reportPossiblyUnboundVariable]
    ee_logger.addHandler(ee_handler)

    # Start the application
    logger.info('Interactive ingest application started.')
    logger.info(f'Logging directory is {logging_dir.resolve()}.')
    if warn_logging_dir_created:
        logger.warning('Logging directory did not yet exist and had to be created by this app.')

    logger.info(f'Configuration file {config_file.resolve()} read. Configuration is:\n{nt.dumps(config.model_dump(mode='json'))}') # pyright: ignore[reportPossiblyUnboundVariable]

    callbacks.set_config(config.model_dump()) # pyright: ignore[reportPossiblyUnboundVariable]
    app        = DashProxy(
                    blueprint=callbacks.blueprint,
                    prevent_initial_callbacks=True,             # type: ignore
                    title='Sensor Data Ingest',
                    update_title=None,                          # Don't change tab title to "Updating..." when the page is being rebuilt
                    # background_callback_manager='diskcache',
                    transforms=[ServersideOutputTransform(), TriggerTransform()],
                 )
    
    app.run(debug=config.application.debug) # pyright: ignore[reportPossiblyUnboundVariable, reportOptionalMemberAccess]

if __name__ == '__main__':
    main()
    logging.shutdown()
