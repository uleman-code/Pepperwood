import os
from pathlib import Path
from ..sensor_data_ingest import config as cfg

this_module: Path = Path(__file__)
config_file: Path = this_module.parent.parent / 'test_files' / 'good_config.toml'
os.environ['INGEST_CONFIG_FILE'] = str(config_file)
cfg.config_init(app_name=this_module.stem)
cfg.logging_init()
cfg.metadata_init()

def test_load_data_good_dat() -> None:
    """Test loading data from a good .dat file."""

    import pandas as pd
    from ..sensor_data_ingest.helpers import load_data, Frames

    # The test data directory is a sibling directory of the tests/ directory
    test_file: str = str(Path(__file__).parent.parent / 'test_files' / 'good_data.dat')
    frames: Frames = load_data(test_file)

    assert isinstance(frames.data, pd.DataFrame) and frames.data.shape == (228, 47)
    assert isinstance(frames.meta, pd.DataFrame) and frames.meta.shape == (47, 3)
    assert isinstance(frames.station, pd.DataFrame) and frames.station.shape == (1, 8)

def test_load_data_good_xlsx() -> None:
    """Test loading data from a good Excel file."""

    import pandas as pd
    from ..sensor_data_ingest.helpers import load_data, Frames

    # The test data directory is one level up from the tests/ directory
    test_file: str = str(Path(__file__).parent.parent / 'test_files' / 'good_data.xlsx')
    frames: Frames = load_data(test_file)

    assert isinstance(frames.data, pd.DataFrame) and frames.data.shape == (228, 47)
    assert isinstance(frames.meta, pd.DataFrame) and frames.meta.shape == (47, 3)
    assert isinstance(frames.station, pd.DataFrame) and frames.station.shape == (1, 8)
    assert isinstance(frames.notes, pd.DataFrame) and frames.notes.shape == (0, 5)


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
    assert matches[0][0].name == 'siteA_20240101.dat' and matches[0][1].name == 'siteA_20240303.csv'
    assert matches[1][0].name == 'siteB-20240202.dat' and matches[1][1].name == 'siteB-20240404.csv'


def test_pair_files_by_prefix_ambiguous_unmatched() -> None:
    """Test that ambiguous matches are left unmatched."""

    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    left_files = [Path('siteA_20240101.dat')]
    right_files = [Path('siteA_20240202.csv'), Path('siteA_20240303.csv')]

    matches = pair_files_by_prefix(left_files, right_files)

    assert matches == []


def test_pair_files_by_prefix_no_match() -> None:
    """Test that files with no shared beginning are not paired."""

    from ..sensor_data_ingest.helpers import pair_files_by_prefix

    left_files = [Path('alpha.dat')]
    right_files = [Path('beta.csv')]

    matches = pair_files_by_prefix(left_files, right_files)

    assert matches == []
