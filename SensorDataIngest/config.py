'''Singleton implementation of configuration settings.

Import this from the main module first, and call config_init there; every subsequent module that imports this
gets access to the result in a clean and threadsafe manner.'''

from typing            import Annotated, Any, Self
from pathlib           import Path
from pydantic          import BaseModel, BeforeValidator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

import logging
import nestedtext as nt

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

class ApplicationCfg(BaseSettings):
    '''General application-wide settings. Should not be sensor- or datalogger-specific.

    All application-level configuration settings, except config_file, normally come from a file but may be overridden using
    commandline arguments or equivalent environment variables.
    For any commandline argument, for example "--debug true", the equivalent environment expression is
    "INGEST_APPLICATION.DEBUG=true" (note the prefix "INGEST_").
    The setting names, whether on the commandline or as environment variables, are not case-sensitive. but the values may well be,
    depending on the specific setting and on the environment. For example, a file path is case-sensitive in MacOS but not in Windows.'''
    
    # BaseSettings (as opposed to BaseModel) looks for environment variables; look for command-line arguments as well.
    model_config: SettingsConfigDict = SettingsConfigDict(cli_parse_args=True, env_prefix='ingest_') # pyright: ignore[reportIncompatibleVariableOverride]

    config_file:                Path         = Path('./<program name>.cfg')     # Just a placeholder for the benefit of --help output
    debug:                      bool         = False
    console_logging_level:      Annotated[int, BeforeValidator(prepare_logging_level)] = logging.getLevelNamesMapping()['INFO']
    file_logging_level:         Annotated[int, BeforeValidator(prepare_logging_level)] = logging.getLevelNamesMapping()['DEBUG']
    logging_directory:          Path         = Path('./logs')

class InputCfg(BaseModel):
    '''Input settings: Anything to do with acceptable input files.'''

    datalogger_file_extensions: list[str]    = ['.dat', '.csv']
    excel_file_extensions:      list[str]    = ['.xlsx', '.xls']

class OutputCfg(BaseModel):
    '''Output settings: Anything to do with the generated output.'''

    worksheet_names: dict[str, str] = dict(data='Data', meta='Columns', site='Meta', notes='Notes')
    data_na_representation: str     = '#N/A'

class MetadataCfg(BaseModel):
    '''Metadata settings: not about the sensor data but only the standard index columns and what's in the other worksheets.
    
    Variable (column) metadata will be handled separately.'''

    timestamp_column:       str
    sequence_number_column: str
    sampling_interval:      str
    description_columns:    list[str]
    site_columns:           list[str]
    notes_columns:          list[str]

class Config(BaseModel):
    '''Combination of all settings/configuration models. None values are only allowed for placeholder initialization.'''
    
    config_version: str            | None = None
    application:    ApplicationCfg | None = None
    input:          InputCfg       | None = None
    output:         OutputCfg      | None = None
    metadata:       MetadataCfg    | None = None

    @model_validator(mode='after')
    def all_or_none(self) -> Self:
        '''Valid if either all submodels (attributes) are initialized, or none are.'''

        if all({val is None for val in self.__dict__.values()}) or all({val is not None for val in self.__dict__.values()}):
            return self
        else:
            raise ValueError(f'If any submodels of Config are initialized, they must all be. The following are not initialized: {[', '.join(k) for k,v in self.__dict__ if v is None]}')

config_model:             Config         = Config()         # Empty model (only Nones)
config:                   dict[str, Any] = {}
capitalized_program_name: str            = ''

def config_init(program_name: str) -> None:
    '''Initialize the configuration settings.
    
    The first step is to identify the settings file. The default is provided by the caller, usually from the main module.
    By using that default to initialize an instance of ApplicationCFg, we automatically get the ability to override it by any value provided
    as a commandline argument or environment variable. Then use the actual file path to read the whole configuration.

    Set the resulting configuration model, a dictionary version of the same, and the program name (capitalized) as globals, to
    make them available to all modules that import this one.

    Parameters:
        program_name    Used in the default name of the configuration settings file, and (capitalized) in log records.

    Terminates the program if there's a validation error or the indicated configuration file cannot be found.
    '''

    global config_model
    global config
    global capitalized_program_name

    capitalized_program_name = program_name.capitalize()

    temp_config: ApplicationCfg = ApplicationCfg(config_file=Path(f'{program_name}.cfg'))        # The default path may be overridden in the environment or on the command line.
    config_file: Path           = temp_config.config_file

    try:
        # Get the configuration from a file in NestedText format.
        keymap = {}
        config_raw: dict[str, Any]           = nt.load(config_file, keymap=keymap, normalize_key=normalize_config_key) # type: ignore
        config_model                         = Config.model_validate(config_raw)
        config_model.application.config_file = config_file                      # pyright: ignore[reportOptionalMemberAccess] # Just for accurate logging purposes
        config                               = config_model.model_dump()        # Expose the configuration settings as a standard dictionary
    except nt.NestedTextError as e:
        e.terminate()

def config_print() -> str:
    '''Create a readable representation of the configuration settings with the same format as the configuration file.'''

    return nt.dumps(config_model.model_dump(mode='json'))