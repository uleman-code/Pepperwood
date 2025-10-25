def test_load_data_good_dat() -> None:
    """Test loading data from a good .dat file."""
    from pathlib import Path

    import base64
    import pandas as pd

    # Configure a minimal config before importing helpers. helpers imports configuration at module import
    # time, so the config must exist first.
    from ..sensor_data_ingest import config as cfg
    from .testconfig import miniconfig

    cfg.config = miniconfig
    cfg.program_name = 'ingest'

    from ..sensor_data_ingest.helpers import load_data

    # The test data directory is one level up from the tests/ directory
    test_file_path: Path = Path(__file__).parent.parent / 'test_files' / 'good_data.dat'
    with open(test_file_path, 'rb') as f:
        test_data: bytes = f.read()

    header = 'data:application/octet-stream;base64'
    contents = ','.join([header, base64.b64encode(test_data).decode('ascii')])

    frames = load_data(contents, test_file_path.name)

    assert isinstance(frames['data'], pd.DataFrame) and frames['data'].shape == (228, 47)
    assert isinstance(frames['meta'], pd.DataFrame) and frames['meta'].shape == (47, 3)
    assert isinstance(frames['station'], pd.DataFrame) and frames['station'].shape == (1, 8)

def test_load_data_good_xlsx() -> None:
    """Test loading data from a good Excel file."""
    from pathlib import Path

    import base64
    import pandas as pd

    # Configure a minimal config before importing helpers. helpers imports configuration at module import
    # time, so the config must exist first.
    from ..sensor_data_ingest import config as cfg
    from .testconfig import miniconfig

    cfg.config = miniconfig
    cfg.program_name = 'Ingest'

    from ..sensor_data_ingest.helpers import load_data

    # The test data directory is one level up from the tests/ directory
    test_file_path: Path = Path(__file__).parent.parent / 'test_files' / 'good_data.xlsx'
    with open(test_file_path, 'rb') as f:
        test_data: bytes = f.read()

    header = 'data:application/octet-stream;base64'
    contents = ','.join([header, base64.b64encode(test_data).decode('ascii')])

    frames = load_data(contents, test_file_path.name)

    assert isinstance(frames['data'], pd.DataFrame) and frames['data'].shape == (228, 47)
    assert isinstance(frames['meta'], pd.DataFrame) and frames['meta'].shape == (47, 3)
    assert isinstance(frames['station'], pd.DataFrame) and frames['station'].shape == (1, 8)
    assert isinstance(frames['notes'], pd.DataFrame) and frames['notes'].shape == (0, 6)