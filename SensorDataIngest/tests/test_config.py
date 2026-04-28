from pathlib import Path
import os
import pytest

def test_load_good_config() -> None:
    """Test loading a good configuration file."""
    from ..sensor_data_ingest import config as cfg
    from ..sensor_data_ingest.config import Config

    this_module: Path = Path(__file__)
    test_file: Path = this_module.parent.parent / 'test_files' / 'good_config.toml'
    os.environ['INGEST_CONFIG_FILE'] = str(test_file)
    cfg.config_init(app_name=this_module.stem)

    assert isinstance(cfg.config, Config)

def test_load_bad_config() -> None:
    """Test loading a bad configuration file."""
    from pathlib import Path

    from ..sensor_data_ingest import config as cfg
    from pydantic import ValidationError

    this_module: Path = Path(__file__)
    test_file: Path = this_module.parent.parent / 'test_files' / 'bad_config.toml'
    os.environ['INGEST_CONFIG_FILE'] = str(test_file)

    with pytest.raises(ValidationError) as exc_info:
        cfg.config_init(app_name=this_module.stem)

    error_msg = str(exc_info.value)
    assert 'Field required' in error_msg
    assert 'host' in error_msg
    assert 'timestamp_column' in error_msg

def test_config_not_found() -> None:
    """Test loading a non-existent configuration file."""
    from pathlib import Path

    from ..sensor_data_ingest import config as cfg

    this_module: Path = Path(__file__)
    test_file: Path = this_module.parent.parent / 'test_files' / 'nonexistent_config.toml'
    os.environ['INGEST_CONFIG_FILE'] = str(test_file)

    with pytest.raises(FileNotFoundError) as exc_info:
        cfg.config_init(app_name=this_module.stem)
    
    error_msg = str(exc_info.value)
    assert 'No such file or directory' in error_msg