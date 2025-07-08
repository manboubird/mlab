#!/usr/bin/env python3
"""
Logging utilities for SceneUnderstanding application.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

# Install rich traceback handler
install(show_locals=True)

console = Console()

class SceneUnderstandingLogger:
    """Custom logger for SceneUnderstanding application."""
    
    def __init__(self, name: str = "scene_understanding"):
        self.logger = logging.getLogger(name)
        self._setup_logging()
    
    def _setup_logging(self, level: str = "INFO", log_file: Optional[str] = None) -> None:
        """Setup logging configuration."""
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Set log level
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Create formatters
        console_formatter = logging.Formatter(
            "%(message)s",
            datefmt="[%X]"
        )
        
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler with rich formatting
        console_handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            markup=True
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (optional)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(message, **kwargs)
    
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(message, **kwargs)
    
    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self.logger.critical(message, **kwargs)


class SceneUnderstandingError(Exception):
    """Base exception for SceneUnderstanding application."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - Details: {self.details}"
        return self.message


class DataLoadError(SceneUnderstandingError):
    """Exception raised when data loading fails."""
    pass


class SchemaError(SceneUnderstandingError):
    """Exception raised when schema operations fail."""
    pass


class SamplingError(SceneUnderstandingError):
    """Exception raised when sampling operations fail."""
    pass


class DatabaseError(SceneUnderstandingError):
    """Exception raised when database operations fail."""
    pass


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> SceneUnderstandingLogger:
    """
    Setup application logging.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = setup_logging("DEBUG", "app.log")
        >>> logger.info("Application started")
    """
    return SceneUnderstandingLogger()._setup_logging(level, log_file)


def log_function_call(func_name: str, **kwargs: Any) -> None:
    """
    Log function call with parameters.
    
    Args:
        func_name: Name of the function being called
        **kwargs: Function parameters to log
        
    Example:
        >>> log_function_call("load_json_data", file_path="data.json", sample_size=1000)
    """
    params = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    console.print(f"[dim]Calling {func_name}({params})[/dim]")


def log_performance(operation: str, duration: float, **kwargs: Any) -> None:
    """
    Log performance metrics.
    
    Args:
        operation: Name of the operation
        duration: Duration in seconds
        **kwargs: Additional metrics to log
        
    Example:
        >>> log_performance("data_loading", 2.5, records_loaded=1000)
    """
    metrics = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    console.print(f"[green]✓ {operation} completed in {duration:.2f}s[/green]")
    if metrics:
        console.print(f"[dim]  Metrics: {metrics}[/dim]")


# Global logger instance
logger = SceneUnderstandingLogger() 