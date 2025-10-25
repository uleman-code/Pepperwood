"""This package implements an interactive data ingest app for Pepperwood Preserve environmental sensor data.

Modules:
    config.py       Configuration management and logging setup
    layout.py       The Dash Mantine page elements and layout
    callbacks.py    The Dash callbacks that implement the page elements' behaviors
    helpers.py      Functions implementing actions that are independent of the Dash
                    environment, called by callbacks
"""

__version__: str       = '0.6'        # TODO: Need a proper versioning setup
__all__:     list[str] = ['config', 'layout', 'callbacks', 'helpers']
