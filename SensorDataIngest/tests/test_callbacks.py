from pathlib import Path
from types import ModuleType

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
    assert result[0].batch_completed is True
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


def test_lifecycle_upload_tracker_prefers_named_module(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    import sys

    from ..sensor_data_ingest import callbacks

    named_lifecycle = ModuleType('named_lifecycle')
    standalone_lifecycle = ModuleType('__main__')
    named_uploads: list[str] = []
    standalone_uploads: list[str] = []
    named_lifecycle.track_upload_id = named_uploads.append
    standalone_lifecycle.track_upload_id = standalone_uploads.append
    monkeypatch.setattr(callbacks.cfg, 'program_name', 'named_lifecycle')
    monkeypatch.setitem(sys.modules, 'named_lifecycle', named_lifecycle)
    monkeypatch.setitem(sys.modules, '__main__', standalone_lifecycle)

    tracker = callbacks._get_lifecycle_upload_tracker()
    assert tracker is not None
    tracker('module-upload')

    assert named_uploads == ['module-upload']
    assert standalone_uploads == []


def test_lifecycle_upload_tracker_falls_back_to_main_module(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    import sys

    from ..sensor_data_ingest import callbacks

    standalone_lifecycle = ModuleType('__main__')
    standalone_uploads: list[str] = []
    standalone_lifecycle.track_upload_id = standalone_uploads.append
    monkeypatch.setattr(callbacks.cfg, 'program_name', 'missing_lifecycle')
    monkeypatch.setitem(sys.modules, '__main__', standalone_lifecycle)

    tracker = callbacks._get_lifecycle_upload_tracker()
    assert tracker is not None
    tracker('script-upload')

    assert standalone_uploads == ['script-upload']


def test_track_upload_id_uses_cached_tracker(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    from ..sensor_data_ingest import callbacks

    tracked_uploads: list[str] = []
    monkeypatch.setattr(callbacks, '_lifecycle_upload_tracker', tracked_uploads.append)

    callbacks._track_upload_id('cached-upload')

    assert tracked_uploads == ['cached-upload']


def test_report_sanity_checks_runs_only_once_without_notes(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    import pandas as pd
    from dash.exceptions import PreventUpdate

    from ..sensor_data_ingest import callbacks, helpers
    from ..sensor_data_ingest.callbacks import Context, QA_Status, report_sanity_checks

    frames = helpers.Frames(
        data=pd.DataFrame({'timestamp': [1], 'seqno': [1], 'value': [1.0]}),
        notes=pd.DataFrame(),
    )
    qa_calls: list[helpers.Frames] = []
    monkeypatch.setattr(callbacks, '_run_sanity_checks', lambda data, _range: qa_calls.append(data) or [])
    context = Context(files=['file.dat'], unsaved=True)

    report, completed_context, _ = report_sanity_checks(None, context, frames)

    assert completed_context.qa_status == QA_Status.COMPLETE
    assert len(qa_calls) == 1

    with pt.raises(PreventUpdate):
        report_sanity_checks(report, completed_context, frames)

    assert len(qa_calls) == 1


def test_report_sanity_checks_skips_saved_data(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    import pandas as pd
    from dash.exceptions import PreventUpdate

    from ..sensor_data_ingest import callbacks, helpers
    from ..sensor_data_ingest.callbacks import Context, report_sanity_checks

    frames = helpers.Frames(
        data=pd.DataFrame({'timestamp': [1], 'seqno': [1], 'value': [1.0]}),
        notes=pd.DataFrame(),
    )
    monkeypatch.setattr(
        callbacks,
        '_run_sanity_checks',
        lambda *_args: pt.fail('Sanity checks should not run after saving.'),
    )

    with pt.raises(PreventUpdate):
        report_sanity_checks(None, Context(files=['file.dat'], unsaved=False), frames)


def test_save_file_shows_badge_with_successful_save(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    import pandas as pd

    from ..sensor_data_ingest import callbacks, helpers
    from ..sensor_data_ingest.callbacks import Context, QA_Status, save_file

    frames = helpers.Frames(
        data=pd.DataFrame({'timestamp': [1], 'seqno': [1], 'value': [1.0]}),
        notes=pd.DataFrame(),
    )
    context = Context(
        files=['file.dat'],
        unsaved=True,
        upload_id='upload-id',
        qa_status=QA_Status.COMPLETE,
    )
    monkeypatch.setattr(callbacks, 'callback_context', SimpleNamespace(triggered_id='save-button'))
    monkeypatch.setattr(callbacks.helpers, 'multi_df_to_excel', lambda *_args: '/download/file.xlsx')

    result = save_file(context, frames)

    assert result[0] == '/download/file.xlsx'
    assert result[1].unsaved is False
    assert result[2] == 'inline'


def test_toggle_save_clear_locks_batch_controls_until_clear() -> None:
    from ..sensor_data_ingest.callbacks import Context, toggle_save_clear

    active_batch = Context(files=['first.dat', 'second.dat'], start_batch=True)
    completed_batch = Context(
        files=['first.dat', 'second.dat'],
        unsaved=False,
        batch_completed=True,
    )
    single_file = Context(files=['single.dat'], unsaved=False)

    assert toggle_save_clear(active_batch) == (True, True, True, False)
    assert toggle_save_clear(completed_batch) == (True, True, True, False)
    assert toggle_save_clear(single_file) == (False, False, False, False)
    assert toggle_save_clear(Context()) == (True, True, True, True)


def test_setup_batch_builds_file_sections_before_save() -> None:
    from ..sensor_data_ingest.callbacks import Context, setup_batch

    result = setup_batch(
        Context(files=['first.dat', 'second.dat'], unsaved=True),
        [
            {'name': 'first.dat', 'size': 1024},
            {'name': 'second.dat', 'size': 2048},
        ],
        [['first.dat', 'first-base.xlsx'], ['second.dat', None]],
        None,
    )

    assert result[1] == ['2 files loaded. Click Save to process.']
    sections = result[4]
    assert [section.id for section in sections] == ['file-info-0', 'file-info-1']

    first_text = sections[0].children.children[1].children
    assert first_text[0].children == r'first.dat $\xrightarrow{+}$ first-base.xlsx'
    assert first_text[1].children[0].children.startswith('Size: ')
    assert sections[0].children.children[0].visible is False
    assert sections[0].children.children[2].children is None


def test_setup_batch_updates_existing_sections_for_append() -> None:
    from ..sensor_data_ingest.callbacks import Context, setup_batch

    original_context = Context(files=['first.dat', 'second.dat'], unsaved=True)
    original_sections = setup_batch(
        original_context,
        [
            {'name': 'first.dat', 'size': 1024},
            {'name': 'second.dat', 'size': 2048},
        ],
        [],
        None,
    )[4]

    result = setup_batch(
        Context(files=['first.dat', 'second.dat'], unsaved=False, start_batch=True),
        [],
        [['first.dat', 'first-base.xlsx'], ['second.dat', 'second-base.xlsx']],
        original_sections,
    )

    sections = result[4]
    filenames = [
        section.children.children[1].children[0].children
        for section in sections
    ]
    assert filenames == [
        r'first.dat $\xrightarrow{+}$ first-base.xlsx',
        r'second.dat $\xrightarrow{+}$ second-base.xlsx',
    ]


def test_setup_batch_updates_serialized_sections_for_append() -> None:
    from ..sensor_data_ingest.callbacks import Context, setup_batch

    def serialize_component(value):
        if hasattr(value, 'to_plotly_json'):
            return serialize_component(value.to_plotly_json())
        if isinstance(value, dict):
            return {key: serialize_component(child) for key, child in value.items()}
        if isinstance(value, list):
            return [serialize_component(child) for child in value]
        return value

    sections = setup_batch(
        Context(files=['first.dat', 'second.dat'], unsaved=True),
        [
            {'name': 'first.dat', 'size': 1024},
            {'name': 'second.dat', 'size': 2048},
        ],
        [],
        None,
    )[4]
    client_sections = serialize_component(sections)

    result = setup_batch(
        Context(files=['first.dat', 'second.dat'], unsaved=False, start_batch=True),
        [],
        [['first.dat', 'first-base.xlsx'], ['second.dat', 'second-base.xlsx']],
        client_sections,
    )

    filenames = [
        section.children.children[1].children[0].children
        for section in result[4]
    ]
    assert filenames == [
        r'first.dat $\xrightarrow{+}$ first-base.xlsx',
        r'second.dat $\xrightarrow{+}$ second-base.xlsx',
    ]


def test_process_batch_runs_qa_for_append_with_existing_notes(
    monkeypatch: pt.MonkeyPatch,
) -> None:
    import pandas as pd
    import dash_mantine_components as dmc

    from ..sensor_data_ingest import callbacks, helpers
    from ..sensor_data_ingest.callbacks import Context, process_batch

    new_frames = helpers.Frames(
        data=pd.DataFrame({'timestamp': [1], 'seqno': [1], 'value': [1.0]}),
        notes=pd.DataFrame(),
    )
    combined_frames = helpers.Frames(
        data=pd.DataFrame({'timestamp': [1, 2], 'seqno': [1, 2], 'value': [1.0, 2.0]}),
        notes=pd.DataFrame({'issue': ['previous QA result']}),
    )
    qa_range = ['2024-01-01 00:00:00', '2024-01-01 00:01:00']
    qa_report = dmc.Text('New data has a gap.')
    set_props_calls = []
    qa_calls = []

    monkeypatch.setattr(helpers, 'load_data', lambda _file: new_frames)
    monkeypatch.setattr(helpers, 'prepare_for_append', lambda _frames: None)
    monkeypatch.setattr(
        helpers,
        'append',
        lambda _base, _new: (combined_frames, qa_range),
    )
    monkeypatch.setattr(
        helpers,
        'multi_df_to_excel',
        lambda *_args: '/download/result.xlsx',
    )
    monkeypatch.setattr(
        callbacks,
        '_run_sanity_checks',
        lambda frames, requested_range: (
            qa_calls.append((frames, requested_range)) or [qa_report]
        ),
    )
    monkeypatch.setattr(
        callbacks,
        'set_props',
        lambda component_id, props: set_props_calls.append((component_id, props)),
    )

    process_batch(
        0,
        Context(files=['new.dat'], upload_id='upload-id'),
        [['new.dat', 'base.xlsx']],
    )

    assert qa_calls == [(combined_frames, qa_range)]
    report_update = next(
        props for component_id, props in set_props_calls
        if component_id == 'sanity-checks-0'
    )
    assert qa_report in report_update['children']
