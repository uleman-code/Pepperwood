"""Main module for the SensorDataIngest application for Pepperwood."""

import atexit
import logging
import shutil
from pathlib import Path
from typing import Final
from urllib.parse import unquote

from flask import abort, send_file
from sensor_data_ingest import config as cfg
from dash_extensions.enrich import (
    DashProxy,
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
            update_title=None,
            )
server = app.server  # noqa: F841  # Expose the Flask server for deployment in the cloud.
file_cache: Path = Path(cfg.config.application.file_cache_root)
ACTIVE_UPLOAD_IDS: set[str] = set()
_SHUTDOWN_CLEANUP_REGISTERED: bool = False


def track_upload_id(upload_id: str | None) -> None:
    """Remember an upload directory that still needs cleanup on shutdown."""
    if upload_id:
        ACTIVE_UPLOAD_IDS.add(str(upload_id))


def untrack_upload_id(upload_id: str | None) -> None:
    """Forget a tracked upload directory once it has been processed or cleared."""
    if upload_id:
        ACTIVE_UPLOAD_IDS.discard(str(upload_id))


def shutdown_cleanup():
    """Perform any cleanup tasks before the application shuts down."""
    logging.info('Shutting down %s application.', Path(__file__).stem)

    for upload_id in sorted(ACTIVE_UPLOAD_IDS):
        upload_root = file_cache / upload_id
        if upload_root.exists():
            logging.info('Removing upload cache directory %s.', upload_root)
            shutil.rmtree(upload_root, ignore_errors=True)
            
        untrack_upload_id(upload_id)

    logging.shutdown()


if not _SHUTDOWN_CLEANUP_REGISTERED:
    atexit.register(shutdown_cleanup)
    _SHUTDOWN_CLEANUP_REGISTERED = True

@server.route('/download/<upload_id>/<path:filename>', methods=['GET'])
def serve_download(upload_id: str, filename: str):
    """Serve a previously generated Excel export directly to the user's browser."""

    download_root = (file_cache / unquote(upload_id) / 'download').resolve()
    if download_root.parent.parent != file_cache.resolve():
        abort(404)

    storage_path = (download_root / unquote(filename)).resolve()
    if download_root not in storage_path.parents or not storage_path.is_file():
        abort(404)

    response = send_file(
        storage_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=Path(filename).name,
        max_age=0,
    )
    response.call_on_close(lambda: storage_path.unlink(missing_ok=True))
    return response

du.configurator(app, cfg.config.application.file_cache_root, use_upload_id=True)

# Create the callbacks after configuring the Uploader, which in turn must happen after creating the app.
from sensor_data_ingest import callbacks  # noqa: E402, F401

if __name__ == '__main__':
    # NOTE: If the Dash app is run with debug=True, this main module is executed twice, resulting
    #       in duplicate logging output.
    #       This has to do with Flask and its support for automatic reloading upon any code changes.
    #       It can be suppressed, at the cost of losing that very convenient reloading behavior.
    #       The duplicate messages do not appear when debug=False.
    app.run(debug=cfg.config.application.debug)
