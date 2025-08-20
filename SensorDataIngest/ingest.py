#! ../.venv/bin/python
'''Main module for the SensorDataIngest package for Pepperwood.'''

import logging

from dash_extensions.enrich import DashProxy, ServersideOutputTransform, TriggerTransform
from dash                   import _dash_renderer

import dash_mantine_components as dmc
import nestedtext              as nt

from enum              import StrEnum
from pathlib           import Path
from pydantic          import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from layout            import layout
from callbacks         import *           # In this case, a star import is acceptable: we want to define all callbacks but won't call them directly.

# Configuration settings
config: dict[str, Any] = {}       # All configuration settings and string literals are defined externally
                                  # (in a file, in the environment, or on the command line)

class LoggingLevel(StrEnum):
    DEBUG    = 'DEBUG'
    INFO     = 'INFO'
    WARNING  = 'WARNING'
    ERROR    = 'ERROR'
    CRITICAL = 'CRITICAL'
class ApplicationCfg(BaseModel):
    config_version:             str
    application_version:        str
    debug:                      bool = False
    logging_level:              LoggingLevel = LoggingLevel.INFO
    logging_directory:          Path         = Path('./logs')
    datalogger_file_extensions: list[str]    = ['.dat', '.csv']
    excel_file_extensions:      list[str]    = ['.xlsx', '.xls']

class OutputCfg(BaseModel):
    worksheet_names: dict[str, str] = dict(data='Data', meta='Columns', site='Meta', notes='Notes')

class MetadataCfg(BaseModel):
    timestamp_column:       str
    sequence_number_column: str
    description_columns:    list[str]
    site_columns:           list[str]
    notes_columns:          list[str]

class Config(BaseSettings):
    model_config: SettingsConfigDict = SettingsConfigDict(cli_parse_args=True, env_prefix='ingest_') # pyright: ignore[reportIncompatibleVariableOverride]
    
    config_file:  Path               = Path('./ingest.cfg')
    application:  ApplicationCfg
    output:       OutputCfg
    metadata:     MetadataCfg

def normalize_config_key(key: str, parent_keys: list[str]) -> str:
    '''Make configuration keys case-insensitive and connect words by underscore.'''

    return '_'.join(key.lower().split())

def main() -> None:
    # Get the configuration from a file in NestedText format.
    filename = './ingest.cfg'
    try:
        keymap = {}
        config_raw: dict[str, Any] = nt.load(filename, keymap=keymap, normalize_key=normalize_config_key) # type: ignore
        config:     Config         = Config.model_validate(config_raw)
    except nt.NestedTextError as e:
        e.terminate()

    # Set up logging.
    # Write logs to a subdirectory of the current directory (from where the script was started).
    # TODO: Set this up to be overridden by a command-line argument
    # If logging_dir does not exist, create it.
    # The current setup errors out if logging_dir exists but is not a directory (because then creating the log file fails).
    current_dir = Path('.')
    logging_dir = current_dir / 'logs'

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

    file_handler.setLevel('DEBUG')
    file_handler.setFormatter(file_formatter)
    stream_handler.setLevel('INFO')
    stream_handler.setFormatter(stream_formatter)

    logger.setLevel('DEBUG')
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Function entry/exit logger: substitute a custom name (the name of the wrapped function) for funcname.
    ee_logger    = logging.getLogger(module_name)
    ee_handler   = logging.FileHandler(logging_dir / (module_name + '.log'))
    ee_formatter = logging.Formatter('{asctime}|{levelname:5s}|{module:9s}|{fname:28s}: {message}', style='{', datefmt='%Y-%m-%d %H:%M:%S')

    ee_handler.setLevel('DEBUG')
    ee_handler.setFormatter(ee_formatter)
    ee_logger.setLevel('DEBUG')
    ee_logger.addHandler(ee_handler)

    # Start the application
    logger.info('Interactive ingest application started.')
    logger.info(f'Logging directory is {logging_dir.resolve()}.')
    if warn_logging_dir_created:
        logger.warning('Logging directory did not yet exist and had to be created by this app.')

    logger.info(f'Configuration file {filename} read. Configuration is:\n{nt.dumps(config.model_dump(mode='json'))}') # pyright: ignore[reportPossiblyUnboundVariable]

    app        = DashProxy(
                    prevent_initial_callbacks=True,             # type: ignore
                    title='Sensor Data Ingest',
                    update_title=None,                          # Does nothing?
                    # background_callback_manager='diskcache',
                    transforms=[ServersideOutputTransform(), TriggerTransform()],
                 )
    app.layout = dmc.MantineProvider(layout)
    
    app.run(debug=True)

if __name__ == '__main__':
    main()
    logging.shutdown()
