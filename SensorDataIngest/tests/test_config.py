from pathlib import Path
import os

def test_load_good_config() -> None:
    """Test loading a good configuration file."""

    from ..sensor_data_ingest import config as cfg
    from ..sensor_data_ingest.config import Config

    this_module: Path = Path(__file__)
    test_file: Path = this_module.parent.parent / 'test_files' / 'good_config.toml'
    os.environ['INGEST_CONFIG_FILE'] = str(test_file)
    cfg.config_init(app_name=this_module.stem)

    # TODO: Set the test config file in the environment, initialize config, then reload the config module.

    assert isinstance(cfg.config, Config)

def test_load_bad_config() -> None:
    """Test loading a bad configuration file."""
    from pathlib import Path

    from ..sensor_data_ingest import config as cfg
    from pydantic import ValidationError

    this_module: Path = Path(__file__)
    test_file: Path = this_module.parent.parent / 'test_files' / 'bad_config.toml'
    os.environ['INGEST_CONFIG_FILE'] = str(test_file)

    try:
        cfg.config_init(app_name=this_module.stem)
    except ValidationError as e:
        error_msg = str(e)

    assert 'Field required' in error_msg
    assert 'host' in error_msg
    assert 'timestamp_column' in error_msg
    # assert 'host' in error_msg

def test_config_not_found() -> None:
    """Test loading a non-existent configuration file."""
    from pathlib import Path

    from ..sensor_data_ingest import config as cfg

    this_module: Path = Path(__file__)
    test_file: Path = this_module.parent.parent / 'test_files' / 'nonexistent_config.toml'
    os.environ['INGEST_CONFIG_FILE'] = str(test_file)

    try:
        cfg.config_init(app_name=this_module.stem)
    except FileNotFoundError as e:
        error_msg = str(e)

    assert 'No such file or directory' in error_msg