#! ../.venv/bin/python
'''Main module for the SensorDataIngest package for Pepperwood.'''

import logging

from dash_extensions.enrich import DashProxy, ServersideOutputTransform, TriggerTransform
from dash                   import _dash_renderer

import dash_mantine_components as dmc

from pathlib   import Path
from layout    import layout
from callbacks import *           # In this case, a star import is acceptable: we want to define all callbacks but won't call them directly.

def main():
    _dash_renderer._set_react_version('18.2.0')     # Required by Dash Mantine Components 0.14.3; the need should go away in a future release.

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
