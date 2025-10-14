"""This package implements an interactive data ingest app for Pepperwood environmental sensor data.

Modules:
    sensor_data_ingest.py   The main program
    layout.py               The Dash Mantine page elements and layout
    callbacks.py            The Dash callbacks that implement the page elements' behaviors
    helpers.py              Functions implementing actions that are independent of the Dash
                            environment, called by callbacks

Usage:
    With the directory containing this package as the current working directory, use this command:
    > python sensor_data_ingest.py

    This starts a web server; using a browser, open localhost:8050 (or whichever port is listed
    in the response to running the script).
"""  # noqa: N999

__version__: str = '0.6'        # Placeholder. Need a proper versioning setup
