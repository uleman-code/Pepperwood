"""Singleton implementation of configuration settings.

Import this from the main module first, and call config_init there; every subsequent module that imports this
gets access to the result in a clean and threadsafe manner."""

from typing import Annotated, Any, Self
from pathlib import Path
from pydantic import BaseModel, BeforeValidator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

import logging
from logging import handlers

import tomllib
import tomli_w


# Configuration settings and validation
def normalize_config_key(key: str, parent_keys: list[str]) -> str:
    """Make configuration keys case-insensitive and connect words by underscore."""

    return '_'.join(key.lower().split())


def to_upper(input: Any) -> str:
    """Expect a string, or at least something that has a string representation; convert to uppercase."""

    return str(input).upper()


def prepare_logging_level(input: Any) -> int:
    """Accept a case-insensitive logging-level string or an integer between 0 and 50; return the (equivalent) integer.

    Strings should be from the familiar logging levels: DEBUG, INFO, WARNING, ERROR (NOTSET and CRITICAL also exist).
    Integers are truncated to the interval [0, 50] (equivalent to [NOTSET, CRITICAL]).

    Raises:
        ValueError  if input is neither an integer nor a valid logging-level string
    """

    try:
        # Keep it within the 0 - 50 range.
        return min(
            max(int(input), logging.getLevelNamesMapping()['NOTSET']),
            logging.getLevelNamesMapping()['CRITICAL'],
        )
    except ValueError:
        pass  # Not an integer; try string next

    try:
        if isinstance(input, str):
            return logging.getLevelNamesMapping()[input.upper()]  # case-insensitive
        else:
            raise ValueError(
                f'"{input}" cannot be a valid logging level. It needs to be a string or an integer.'
            )
    except KeyError:
        raise ValueError(
            f'"{input}" is not a valid logging level. Use DEBUG, INFO, WARNING, or ERROR'
        )


class ApplicationCfg(BaseSettings):
    """General application-wide settings. Should not be sensor- or datalogger-specific.

    All application-level configuration settings, except config_file, normally come from a file but may be overridden using
    commandline arguments or equivalent environment variables.
    For any commandline argument, for example "--debug true", the equivalent environment expression is
    "INGEST_APPLICATION.DEBUG=true" (note the prefix "INGEST_").
    The setting names, whether on the commandline or as environment variables, are not case-sensitive. but the values may well be,
    depending on the specific setting and on the environment. For example, a file path is case-sensitive in MacOS but not in Windows."""

    # BaseSettings (as opposed to BaseModel) looks for environment variables; look for command-line arguments as well.
    # TODO: Is there any way around hardcoding the 'ingest_' prefix here?
    model_config: SettingsConfigDict = SettingsConfigDict(cli_parse_args=True, env_prefix='ingest_')

    config_file: Path = Path(
        './<program name>.toml'
    )  # Just a placeholder for the benefit of --help output
    debug: bool = False
    console_logging_level: Annotated[int, BeforeValidator(prepare_logging_level)] = (
        logging.getLevelNamesMapping()['INFO']
    )
    file_logging_level: Annotated[int, BeforeValidator(prepare_logging_level)] = (
        logging.getLevelNamesMapping()['DEBUG']
    )
    logging_directory: Path = Path('./logs')
    logfile_max_size: int = 10 * 1024 * 1024
    logfile_backup_count: int = 3


class InputCfg(BaseModel):
    """Input settings: Anything to do with acceptable input files."""

    datalogger_file_extensions: list[str] = ['.dat', '.csv']
    excel_file_extensions: list[str] = ['.xlsx', '.xls']


class OutputCfg(BaseModel):
    """Output settings: Anything to do with the generated output."""

    worksheet_names: dict[str, str] = {}
    data_na_representation: str = '#N/A'


class MetadataCfg(BaseModel):
    """Metadata settings: not about the sensor data but only the standard index columns and what's in the other worksheets.

    Variable (column) metadata will be handled separately."""

    timestamp_column: str
    sequence_number_column: str
    sampling_interval: str
    variable_description_columns: list[str]
    station_columns: list[str]
    notes_columns: list[str]


class Config(BaseModel):
    """Combination of all settings/configuration models. None values are only allowed for placeholder initialization, not in an actual config file."""

    config_version: str | None = None
    application: ApplicationCfg | None = None
    input: InputCfg | None = None
    output: OutputCfg | None = None
    metadata: MetadataCfg | None = None

    @model_validator(mode='after')
    def all_or_none(self) -> Self:
        """Valid if either all submodels (attributes) are initialized, or none are."""

        if all({val is None for val in self.__dict__.values()}) or all(
            {val is not None for val in self.__dict__.values()}
        ):
            return self
        else:
            raise ValueError(
                f'If any submodels of Config are initialized, they must all be. The following are not initialized: {[", ".join(k) for k, v in self.__dict__ if v is None]}'
            )


config_model: Config = Config()
config: dict[str, Any] = {}
program_name: str = ''

def config_print() -> str:
    """Return a readable representation of the configuration settings with the same format as the configuration file."""

    return tomli_w.dumps(
        config_model.model_dump(mode='json')
    )  # JSON mode to avoid non-serializable values

def logging_init() -> None:
    """Set up the loggers. Call after initializing the configuration settings (for the logging directory).

    If logging_dir does not exist, create it.

    Limitations:
        Errors out if logging_dir exists but is not a directory (because then
        creating the log file fails).
    """

    app_config: dict[str, Any] = config['application']   # Just a clutter reducing convenience
    logging_dir: Path          = app_config['logging_directory']

    if logging_dir.exists():
        warn_logging_dir_created = False
    else:
        logging_dir.mkdir()
        warn_logging_dir_created = True

    # General-purpose logger
    logger           = logging.getLogger(program_name)
    file_handler     = handlers.RotatingFileHandler(logging_dir / (program_name + '.log'),
                                                    maxBytes=app_config['logfile_max_size'],
                                                    backupCount=app_config['logfile_backup_count'])
    stream_handler   = logging.StreamHandler()
    file_formatter   = logging.Formatter('{asctime}|{levelname:5s}|{module:9s}|{funcName:28s}: ' +
                                         '{message}', style='{', datefmt='%Y-%m-%d %H:%M:%S')
    stream_formatter = logging.Formatter('{levelname}|{name}: {message}', style='{')

    file_handler.setLevel(app_config['file_logging_level'])
    file_handler.setFormatter(file_formatter)
    stream_handler.setLevel(app_config['console_logging_level'])
    stream_handler.setFormatter(stream_formatter)

    logger.setLevel(app_config['file_logging_level'])
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Function entry/exit logger: fname substitutes a custom name (the name of the wrapped function,
    # instead of the wrapper itself) for funcName.
    ee_logger    = logging.getLogger(program_name + '_ee')
    ee_handler   = logging.FileHandler(logging_dir / (program_name + '.log'))
    ee_formatter = logging.Formatter('{asctime}|{levelname:5s}|{module:9s}|{fname:28s}: {message}',
                                     style='{', datefmt='%Y-%m-%d %H:%M:%S')

    ee_handler.setLevel(app_config['file_logging_level'])
    ee_handler.setFormatter(ee_formatter)
    ee_logger.setLevel(app_config['file_logging_level'])
    ee_logger.addHandler(ee_handler)

    # Start the application
    logger.info('Interactive ingest application started.')
    logger.info('Logging directory is %s', logging_dir.resolve())
    if warn_logging_dir_created:
        logger.warning('Logging directory did not yet exist and had to be created by this app.')

    logger.info('Configuration file %s read. Configuration is:\n%s',
                app_config['config_file'].resolve(), config_print())

def config_init(app_name: str) -> None:
    """Initialize the configuration settings.

    The first step is to identify the settings file. The default is provided by the caller, usually from the main module.
    By using that default to initialize an instance of ApplicationCFg, we automatically get the ability to override it by any value provided
    as a commandline argument or environment variable. Then use the actual file path to read the whole configuration.

    Set the resulting configuration model, a dictionary version of the same, and the program name (capitalized) as globals, to
    make them available to all modules that import this one.

    Parameters:
        app_name    Used in the default name of the configuration settings file, and in logger names.

    Terminates the program (by not catching any exceptions) if there's a validation error or the indicated configuration file cannot be found.
    """

    global config_model
    global config
    global program_name

    program_name = app_name

    # Find the config file. By running it through pydantic, the default config-file path
    # may be overridden in the environment or on the command line.
    temp_config: ApplicationCfg = ApplicationCfg(config_file=Path(f'{app_name}.toml'))
    config_file: Path = temp_config.config_file

    with config_file.open('rb') as f:
        config_raw: dict[str, Any] = tomllib.load(f)

    config_model = Config.model_validate(config_raw)
    config_model.application.config_file = config_file  # Just for accurate logging purposes
    config = config_model.model_dump()  # Expose the configuration settings as a standard dictionary

    # Logging needs the configuration to be complete to get the logging directory.
    logging_init()
