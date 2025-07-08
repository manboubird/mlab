#!/usr/bin/env python3
"""
Unit tests for configuration management.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch

from src.scene_understanding.config import Config


class TestConfig:
    """Test configuration management."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = Config()
        
        assert config.data_dir == Path("data/01_raw")
        assert config.output_dir == Path("output")
        assert config.sample_size == 1000
        assert config.enable_sampling is True
        assert config.small_dataset_mode is False
        assert config.default_db_format == "sqlite"
        assert config.log_level == "INFO"
        assert config.verbose is False
        assert config.batch_size == 1000
        assert config.max_workers is None
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = Config(
            data_dir=Path("custom/data"),
            sample_size=500,
            enable_sampling=False,
            verbose=True
        )
        
        assert config.data_dir == Path("custom/data")
        assert config.sample_size == 500
        assert config.enable_sampling is False
        assert config.verbose is True
    
    @patch.dict(os.environ, {
        "SCENE_DATA_DIR": "/custom/data",
        "SCENE_SAMPLE_SIZE": "2000",
        "SCENE_ENABLE_SAMPLING": "false",
        "SCENE_VERBOSE": "true"
    })
    def test_from_env(self):
        """Test loading configuration from environment variables."""
        config = Config.from_env()
        
        assert config.data_dir == Path("/custom/data")
        assert config.sample_size == 2000
        assert config.enable_sampling is False
        assert config.verbose is True
    
    def test_validation_success(self):
        """Test successful configuration validation."""
        config = Config(
            data_dir=Path("tests/data"),
            schema_dir=Path("tests/data"),
            sample_size=1000,
            batch_size=500
        )
        
        # Should not raise any exception
        config.validate()
    
    def test_validation_data_dir_not_exists(self):
        """Test validation failure when data directory doesn't exist."""
        config = Config(data_dir=Path("nonexistent/dir"))
        
        with pytest.raises(ValueError, match="Data directory does not exist"):
            config.validate()
    
    def test_validation_schema_dir_not_exists(self):
        """Test validation failure when schema directory doesn't exist."""
        config = Config(schema_dir=Path("nonexistent/dir"))
        
        with pytest.raises(ValueError, match="Schema directory does not exist"):
            config.validate()
    
    def test_validation_invalid_sample_size(self):
        """Test validation failure with invalid sample size."""
        config = Config(sample_size=0)
        
        with pytest.raises(ValueError, match="Sample size must be positive"):
            config.validate()
        
        config.sample_size = -1
        with pytest.raises(ValueError, match="Sample size must be positive"):
            config.validate()
    
    def test_validation_invalid_batch_size(self):
        """Test validation failure with invalid batch size."""
        config = Config(batch_size=0)
        
        with pytest.raises(ValueError, match="Batch size must be positive"):
            config.validate()
        
        config.batch_size = -1
        with pytest.raises(ValueError, match="Batch size must be positive"):
            config.validate()
    
    def test_validation_invalid_max_workers(self):
        """Test validation failure with invalid max workers."""
        config = Config(max_workers=0)
        
        with pytest.raises(ValueError, match="Max workers must be positive"):
            config.validate()
        
        config.max_workers = -1
        with pytest.raises(ValueError, match="Max workers must be positive"):
            config.validate()
    
    def test_to_dict(self):
        """Test converting configuration to dictionary."""
        config = Config(
            data_dir=Path("test/data"),
            sample_size=500,
            enable_sampling=False
        )
        
        config_dict = config.to_dict()
        
        assert config_dict["data_dir"] == "test/data"
        assert config_dict["sample_size"] == 500
        assert config_dict["enable_sampling"] is False
        assert isinstance(config_dict, dict)
    
    @patch('src.scene_understanding.config.console')
    def test_print_summary(self, mock_console):
        """Test printing configuration summary."""
        config = Config(
            data_dir=Path("test/data"),
            sample_size=500,
            enable_sampling=False
        )
        
        config.print_summary()
        
        # Verify console.print was called
        assert mock_console.print.called 