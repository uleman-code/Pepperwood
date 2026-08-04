from pathlib import Path
import pytest as pt
from ..sensor_data_ingest import config as cfg


@pt.fixture(scope='session', autouse=True)
def init_config():
    """Initialize configuration for all tests in this module."""
    this_module: Path = Path(__file__)
    config_file: Path = this_module.parent.parent / 'test_files' / 'good_config.toml'
    pt.monkeypatch.setenv('INGEST_CONFIG_FILE', str(config_file))
    cfg.config_init(app_name=this_module.stem)
    cfg.logging_init()
    cfg.metadata_init()
    yield


def test_load_data_good_dat() -> None:
    """Test loading data from a good .dat file."""
    import pandas as pd
    from ..sensor_data_ingest.helpers import load_data, Frames

    # The test data directory is a sibling directory of the tests/ directory
    test_file: str = str(Path(__file__).parent.parent / 'test_files' / 'good_data.dat')
    frames: Frames = load_data(test_file)

    assert isinstance(frames.data, pd.DataFrame)
    assert frames.data.shape == (228, 47)
    assert isinstance(frames.meta, pd.DataFrame)
    assert frames.meta.shape == (47, 3)
    assert isinstance(frames.station, pd.DataFrame)
    assert frames.station.shape == (1, 8)


def test_load_data_good_xlsx() -> None:
    """Test loading data from a good Excel file."""
    import pandas as pd
    from ..sensor_data_ingest.helpers import load_data, Frames

    # The test data directory is one level up from the tests/ directory
    test_file: str = str(Path(__file__).parent.parent / 'test_files' / 'good_data.xlsx')
    frames: Frames = load_data(test_file)

    assert isinstance(frames.data, pd.DataFrame)
    assert frames.data.shape == (228, 47)
    assert isinstance(frames.meta, pd.DataFrame)
    assert frames.meta.shape == (47, 3)
    assert isinstance(frames.station, pd.DataFrame)
    assert frames.station.shape == (1, 8)
    assert isinstance(frames.notes, pd.DataFrame)
    assert frames.notes.shape == (0, 5)


def test_load_data_missing_file() -> None:
    """Test that load_data raises FileNotFoundError for a non-existent file."""
    from ..sensor_data_ingest.helpers import load_data

    nonexistent_file = str(Path(__file__).parent.parent / 'test_files' / 'nonexistent_data.dat')

    with pt.raises(FileNotFoundError):
        load_data(nonexistent_file)


def test_load_data_corrupt_file(tmp_path) -> None:
    """Test that load_data raises BadFileError for a malformed/corrupted file."""
    from ..sensor_data_ingest.helpers import load_data, BadFileError

    # Create a temporary file with invalid CSV structure
    bad_file_path = tmp_path / 'corrupt_data.dat'
    bad_file_path.write_text('This is not valid CSV data\nJust some random text\n')

    with pt.raises(BadFileError):
        load_data(bad_file_path)


def test_load_data_unsupported_extension() -> None:
    """Test that load_data raises UnsupportedFileTypeError for unsupported file extensions."""
    from ..sensor_data_ingest.helpers import load_data, UnsupportedFileTypeError

    # Use a file path with an unsupported extension (doesn't need to exist)
    unsupported_file = str(Path(__file__).parent.parent / 'test_files' / 'data.txt')

    with pt.raises(UnsupportedFileTypeError) as exc_info:
        load_data(unsupported_file)

    assert '.txt' in str(exc_info.value)


def test_pair_files_by_prefix_unique_match() -> None:
    """Test that files with unique beginning-name matches are paired."""
    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    left_files = [
        Path('siteA_20240101.dat'),
        Path('siteB-20240202.dat'),
    ]
    right_files = [
        Path('siteA_20240303.csv'),
        Path('siteB-20240404.csv'),
    ]

    matches = pair_files_by_prefix(left_files, right_files)

    assert len(matches) == 2
    assert matches[0] == (Path('siteA_20240101.dat'), Path('siteA_20240303.csv'))
    assert matches[1] == (Path('siteB-20240202.dat'), Path('siteB-20240404.csv'))


def test_pair_files_by_prefix_ambiguous_unmatched() -> None:
    """Test that ambiguous matches are left unmatched."""
    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    left_files = [Path('siteA_20240101.dat')]
    right_files = [Path('siteA_20240202.csv'), Path('siteA_20240303.csv')]

    matches = pair_files_by_prefix(left_files, right_files)
    assert matches == []


def test_pair_files_by_prefix_ambiguous_left_unmatched() -> None:
    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    matches = pair_files_by_prefix(
        [Path('siteA_20240101.dat'), Path('siteA_20240202.dat')],
        [Path('siteA_20240303.csv')],
    )

    assert matches == []


def test_pair_files_by_prefix_no_match() -> None:
    """Test that files with no shared beginning are not paired."""
    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    left_files = [Path('alpha.dat')]
    right_files = [Path('beta.csv')]

    matches = pair_files_by_prefix(left_files, right_files)
    assert matches == []


def test_pair_files_by_prefix_empty_inputs() -> None:
    """Test that pair_files_by_prefix returns empty list for empty input lists."""
    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    # Empty left list
    assert pair_files_by_prefix([], [Path('siteA.csv')]) == []

    # Empty right list
    assert pair_files_by_prefix([Path('siteA.dat')], []) == []

    # Both empty
    assert pair_files_by_prefix([], []) == []
