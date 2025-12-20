from pathlib import Path
import sys

def test_load_good_config() -> None:
    """Test loading a good configuration file."""

    from ..sensor_data_ingest import config as cfg
    from ..sensor_data_ingest.config import ConfigModel

    test_file: Path = Path(__file__).parent.parent / 'test_files' / 'good_config.toml'
    sys.environ['INGEST_CONFIG_FILE'] = str(test_file)
    cfg.config_init(app_name=__file__.stem)

    # TODO: Set the test config file in the environment, initialize config, then reload the config module.

    assert isinstance(cfg.config, ConfigModel)

def test_load_bad_config() -> None:
    """Test loading a bad configuration file."""
    from pathlib import Path

    from ..sensor_data_ingest import config as cfg
    from pydantic import ValidationError

    test_file_path: Path = Path(__file__).parent.parent / 'test_files' / 'bad_config.toml'

    try:
        cfg.load_config(test_file_path)
    except ValidationError as e:
        error_msg = str(e)

    assert 'field required' in error_msg
    assert 'logging_dir' in error_msg
    assert 'host' in error_msg

def test_config_not_found() -> None:
    """Test loading a non-existent configuration file."""
    from pathlib import Path

    from ..sensor_data_ingest import config as cfg

    test_file_path: Path = Path(__file__).parent.parent / 'test_files' / 'nonexistent_config.toml'

    try:
        cfg.load_config(test_file_path)
    except FileNotFoundError as e:
        error_msg = str(e)

    assert 'No such file or directory' in error_msg