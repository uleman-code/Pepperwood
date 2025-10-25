"""Main module for the SensorDataIngest application for Pepperwood."""

import logging
from pathlib import Path

from sensor_data_ingest import config as cfg
from dash_extensions.enrich import (
    DashProxy,
    ServersideOutputTransform,
    TriggerTransform,
)


def main() -> None:
    """Configure the application and start the Dash process."""
    # Initialize the configuration module before importing any other modules from this project.
    module_name = Path(__file__).stem
    cfg.config_init(app_name=module_name)

    # Now that the config is initialized, we can import other modules.
    # This import has as a side-effect that it registers all the Dash callbacks.
    from sensor_data_ingest.callbacks import blueprint

    app: DashProxy = DashProxy(
                blueprint=blueprint,
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
    app.run(debug=cfg.config['application']['debug'])

if __name__ == '__main__':
    main()
    logging.shutdown()
