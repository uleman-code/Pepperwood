from pathlib import Path

import pytest as pt


@pt.fixture(scope='module', autouse=True)
def init_config():
    """Initialize configuration needed to import the callbacks module."""
    from ..sensor_data_ingest import config as cfg

    config_file = Path(__file__).parent.parent / 'test_files' / 'good_config.toml'
    with pt.MonkeyPatch.context() as mp:
        mp.setenv('INGEST_CONFIG_FILE', str(config_file))
        cfg.config_init(app_name=Path(__file__).stem)
        cfg.logging_init()
        cfg.metadata_init()
        yield


def test_batch_done_starts_downloads_for_successful_files() -> None:
    from ..sensor_data_ingest.callbacks import Context, batch_done

    context = Context(
        files=['first.dat', 'failed.dat', 'last.dat'],
        unsaved=True,
        upload_id='upload-id',
        start_batch=True,
    )

    result = batch_done(
        context,
        ['inline', 'inline', 'inline'],
        ['/download/upload-id/first.xlsx', None, '/download/upload-id/last.xlsx'],
    )

    assert result[0].unsaved is False
    assert result[0].start_batch is False
    assert [frame.src for frame in result[3]] == [
        '/download/upload-id/first.xlsx',
        '/download/upload-id/last.xlsx',
    ]


def test_batch_done_waits_for_all_files() -> None:
    from dash.exceptions import PreventUpdate

    from ..sensor_data_ingest.callbacks import Context, batch_done

    with pt.raises(PreventUpdate):
        batch_done(
            Context(files=['first.dat', 'last.dat'], unsaved=True, start_batch=True),
            ['inline', 'none'],
            ['/download/upload-id/first.xlsx', None],
        )
