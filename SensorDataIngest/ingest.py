#! ../.venv/bin/python
'''Main module for the SensorDataIngest package for Pepperwood.'''

import logging

from dash import Dash, _dash_renderer

import dash_mantine_components as dmc

from layout    import layout
from callbacks import *           # In this case, a star import is acceptable: we want to instantiate all callbacks.

_dash_renderer._set_react_version('18.2.0')     # Required by Dash Mantine Components 0.14.3; the need should go away in a future release.

# Explicitly identifying the needed stylesheets is mandatory in dash 3.14.3. But this app doesn't use any of them.
stylesheets = [
    # 'https://unpkg.com/@mantine/dates@7/styles.css',
    # 'https://unpkg.com/@mantine/code-highlight@7/styles.css',
    # 'https://unpkg.com/@mantine/charts@7/styles.css',
    # 'https://unpkg.com/@mantine/carousel@7/styles.css',
    # 'https://unpkg.com/@mantine/notifications@7/styles.css',
    # 'https://unpkg.com/@mantine/nprogress@7/styles.css',
]

# Set up logging.
# Write logs to a subdirectory of the current directory (from where the script was started).
# TODO: Set this up to be overridden by a command-line argument
# If logging_dir does not exist, create it.
# The current setup errors out if logging_dir exists but is not a directory (because then creating the log file fails).
current_dir = Path('.')
logging_dir = current_dir / 'logs'

warn_logging_dir_created = False
if not logging_dir.exists():
    logging_dir.mkdir()
    warn_logging_dir_created = True

module_name = Path(__file__).name

logger = logging.getLogger(module_name.capitalize())
file_handler     = logging.FileHandler(logging_dir / (module_name + '.log'))
stream_handler   = logging.StreamHandler()
file_formatter   = logging.Formatter('{asctime}|{name}|{levelname} {message}', style='{', datefmt='%Y-%m-%d %H:%M:%S')
stream_formatter = logging.Formatter('{name}|{levelname} {message}', style='{')

file_handler.setLevel('DEBUG')
file_handler.setFormatter(file_formatter)
stream_handler.setLevel('INFO')
stream_handler.setFormatter(stream_formatter)
logger.setLevel('DEBUG')
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.info('Interactive ingest application started.')
logger.info(f'Logging directory is {logging_dir.resolve()}.')
if warn_logging_dir_created:
    logger.warning('Logging directory did not yet exist and had to be created by this app.')

# Standard creation of a Dash app with Dash Mantine Components.
app = Dash(external_stylesheets=stylesheets)
app.layout = dmc.MantineProvider(layout)

if __name__ == '__main__':
    app.run(debug=True)
