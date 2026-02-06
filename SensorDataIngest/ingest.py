"""Main module for the SensorDataIngest application for Pepperwood."""

import logging
from pathlib import Path
from typing import Final

from sensor_data_ingest import config as cfg
from dash_extensions.enrich import (
    DashProxy,
    ServersideOutputTransform,
    TriggerTransform,
    DataclassTransform,
)
import dash_uploader_uppy5 as du

FLASK_LOGGER: Final = 'werkzeug'

# Initialize the configuration module before importing any other modules from this project.
cfg.config_init(app_name=Path(__file__).stem)
cfg.logging_init()
cfg.metadata_init()
logging.getLogger(FLASK_LOGGER).setLevel(logging.WARNING)  # Suppress endless GET and POST logs from Flask

# Now that the config is initialized, we can import other modules.
# This import has as a side-effect that it registers all the Dash callbacks.
from sensor_data_ingest.layout import blueprint  # noqa: E402

# The following two statements must be at the module level, not inside a function.
# Otherwise, the Dash app won't be properly discovered by deployment tools.
app: DashProxy = DashProxy(
            blueprint=blueprint,
            prevent_initial_callbacks=True,
            title='Sensor Data Ingest',
            update_title=None,                          # While rebuilding the page, don't
                                                        # change tab title to "Updating..."
            # background_callback_manager='diskcache',  # noqa: ERA001
            transforms=[ServersideOutputTransform(), TriggerTransform(), DataclassTransform()],
            )
server = app.server  # noqa: F841  # Expose the Flask server for deployment in the cloud.

du.configurator(app, cfg.config.application.file_cache_root, use_upload_id=False)

# Create the callbacks after configuring the Uploader, which in turn must happen after creating the app.
from sensor_data_ingest import callbacks  # noqa: E402, F401

if __name__ == '__main__':
    # NOTE: If the Dash app is run with debug=True, this main module is executed twice, resulting
    #       in duplicate logging output.
    #       This has to do with Flask and its support for automatic reloading upon any code changes.
    #       It can be suppressed, at the cost of losing that very convenient reloading behavior.
    #       The duplicate messages do not appear when debug=False.
    try:
        app.run(debug=cfg.config.application.debug)
    except KeyboardInterrupt:
        logging.shutdown()
