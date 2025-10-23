"""Main module for the SensorDataIngest application for Pepperwood."""

import logging
from logging import handlers
from pathlib import Path
from typing import Any

import config
from dash_extensions.enrich import (
    DashProxy,
    ServersideOutputTransform,
    TriggerTransform,
)

# Initialize the configuration module before importing any other modules from this project.
module_name = Path(__file__).stem
config.config_init(program_name=module_name)
app_config: dict[str, Any] = config.config['application']   # Just a clutter reducing convenience

# This module, and any others it in turn imports, now has access to the fully initialized
# config module.
import callbacks    # noqa: E402, I001


def main() -> None:
    """Set up the loggers and start the Dash app."""
    # Set up logging.
    # If logging_dir does not exist, create it.
    # The current setup errors out if logging_dir exists but is not a directory (because then
    # creating the log file fails).
    logging_dir: Path = app_config['logging_directory']

    if not logging_dir.exists():
        logging_dir.mkdir()
        warn_logging_dir_created = True
    else:
        warn_logging_dir_created = False

    # General-purpose logger
    logger           = logging.getLogger(module_name.capitalize())
    file_handler     = handlers.RotatingFileHandler(logging_dir / (module_name + '.log'),
                                                    maxBytes=app_config['logfile_max_size'],
                                                    backupCount=app_config['logfile_backup_count'])
    stream_handler   = logging.StreamHandler()
    file_formatter   = logging.Formatter('{asctime}|{levelname:5s}|{module:9s}|{funcName:28s}:'
                                         '{message}', style='{', datefmt='%Y-%m-%d %H:%M:%S')
    stream_formatter = logging.Formatter('{levelname}|{name}: {message}', style='{')

    file_handler.setLevel(app_config['file_logging_level'])
    file_handler.setFormatter(file_formatter)
    stream_handler.setLevel(app_config['console_logging_level'])
    stream_handler.setFormatter(stream_formatter)

    logger.setLevel(app_config['file_logging_level'])
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Function entry/exit logger: fname substitutes a custom name (the name of the wrapped function)
    # for funcName.
    ee_logger    = logging.getLogger(module_name)
    ee_handler   = logging.FileHandler(logging_dir / (module_name + '.log'))
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
                app_config['config_file'].resolve(), config.config_print())

    app: DashProxy = DashProxy(
                blueprint=callbacks.blueprint,
                prevent_initial_callbacks=True,
                title='Sensor Data Ingest',
                update_title=None,                          # While rebuilding the page, don't
                                                            # change tab title to "Updating..."
                # background_callback_manager='diskcache',  # noqa: ERA001
                transforms=[ServersideOutputTransform(), TriggerTransform()],
               )

    # NOTE: If the Dash app is run with debug=True, this main module is executed twice, resulting
    #       in duplicate logging output.
    #       This has to do with Flask and its support for automatic reloading upon any code changes.
    #       It can be suppressed, at the cost of losing that very convenient reloading behavior.
    #       The duplicate messages do not appear when debug=False.
    app.run(debug=app_config['debug'])

if __name__ == '__main__':
    main()
    logging.shutdown()
