#!/usr/bin/env python3
"""
Unit tests for logging utilities.
"""

import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.scene_understanding.utils.logging import (
    SceneUnderstandingLogger,
    SceneUnderstandingError,
    DataLoadError,
    SchemaError,
    SamplingError,
    DatabaseError,
    setup_logging,
    log_function_call,
    log_performance
)


class TestSceneUnderstandingLogger:
    """Test SceneUnderstandingLogger class."""
    
    def test_logger_initialization(self):
        """Test logger initialization."""
        logger = SceneUnderstandingLogger("test_logger")
        
        assert logger.logger.name == "test_logger"
        assert isinstance(logger.logger, logging.Logger)
    
    @patch('src.scene_understanding.utils.logging.console')
    def test_info_logging(self, mock_console):
        """Test info level logging."""
        logger = SceneUnderstandingLogger("test")
        logger.info("Test info message")
        
        # Verify logger has handlers
        assert len(logger.logger.handlers) > 0
    
    @patch('src.scene_understanding.utils.logging.console')
    def test_warning_logging(self, mock_console):
        """Test warning level logging."""
        logger = SceneUnderstandingLogger("test")
        logger.warning("Test warning message")
        
        # Verify logger has handlers
        assert len(logger.logger.handlers) > 0
    
    @patch('src.scene_understanding.utils.logging.console')
    def test_error_logging(self, mock_console):
        """Test error level logging."""
        logger = SceneUnderstandingLogger("test")
        logger.error("Test error message")
        
        # Verify logger has handlers
        assert len(logger.logger.handlers) > 0
    
    def test_setup_logging_with_file(self, temp_dir):
        """Test logging setup with file handler."""
        log_file = temp_dir / "test.log"
        
        logger = setup_logging("INFO", str(log_file))
        
        # Test that log file is created when we log something
        logger.info("Test message")
        assert log_file.exists()
    
    def test_setup_logging_without_file(self):
        """Test logging setup without file handler."""
        logger = setup_logging("DEBUG")
        
        # Should not raise any exception
        assert logger is not None


class TestSceneUnderstandingError:
    """Test custom exception classes."""
    
    def test_base_error(self):
        """Test base SceneUnderstandingError."""
        error = SceneUnderstandingError("Test error message")
        
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.details == {}
    
    def test_base_error_with_details(self):
        """Test base error with details."""
        details = {"file": "test.json", "line": 10}
        error = SceneUnderstandingError("Test error", details)
        
        assert "Test error - Details:" in str(error)
        assert error.details == details
    
    def test_data_load_error(self):
        """Test DataLoadError."""
        error = DataLoadError("Failed to load data")
        
        assert isinstance(error, SceneUnderstandingError)
        assert str(error) == "Failed to load data"
    
    def test_schema_error(self):
        """Test SchemaError."""
        error = SchemaError("Invalid schema")
        
        assert isinstance(error, SceneUnderstandingError)
        assert str(error) == "Invalid schema"
    
    def test_sampling_error(self):
        """Test SamplingError."""
        error = SamplingError("Sampling failed")
        
        assert isinstance(error, SceneUnderstandingError)
        assert str(error) == "Sampling failed"
    
    def test_database_error(self):
        """Test DatabaseError."""
        error = DatabaseError("Database connection failed")
        
        assert isinstance(error, SceneUnderstandingError)
        assert str(error) == "Database connection failed"


class TestLoggingUtilities:
    """Test logging utility functions."""
    
    @patch('src.scene_understanding.utils.logging.console')
    def test_log_function_call(self, mock_console):
        """Test log_function_call utility."""
        log_function_call("test_function", param1="value1", param2=42)
        
        mock_console.print.assert_called_once()
        call_args = mock_console.print.call_args[0][0]
        assert "test_function" in call_args
        assert "param1=value1" in call_args
        assert "param2=42" in call_args
    
    @patch('src.scene_understanding.utils.logging.console')
    def test_log_performance(self, mock_console):
        """Test log_performance utility."""
        log_performance("test_operation", 2.5, records_processed=1000)
        
        # Should be called twice: once for completion, once for metrics
        assert mock_console.print.call_count == 2
        
        # Check completion message
        completion_call = mock_console.print.call_args_list[0]
        assert "test_operation completed in 2.50s" in completion_call[0][0]
        
        # Check metrics message
        metrics_call = mock_console.print.call_args_list[1]
        assert "records_processed=1000" in metrics_call[0][0]
    
    @patch('src.scene_understanding.utils.logging.console')
    def test_log_performance_no_metrics(self, mock_console):
        """Test log_performance without additional metrics."""
        log_performance("test_operation", 1.0)
        
        # Should be called only once for completion
        mock_console.print.assert_called_once()
        call_args = mock_console.print.call_args[0][0]
        assert "test_operation completed in 1.00s" in call_args


class TestLoggerIntegration:
    """Test logger integration with different log levels."""
    
    def test_log_levels(self):
        """Test different log levels."""
        logger = SceneUnderstandingLogger("test")
        
        # Test all log levels
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")
        
        # Should not raise any exceptions
        assert True
    
    def test_logger_with_rich_handler(self):
        """Test logger with rich handler."""
        logger = SceneUnderstandingLogger("test")
        
        # Verify rich handler is present
        handlers = logger.logger.handlers
        rich_handlers = [h for h in handlers if hasattr(h, 'console')]
        
        assert len(rich_handlers) > 0 