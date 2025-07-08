#!/usr/bin/env python3
"""
Configuration management for SceneUnderstanding application.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any
from rich.console import Console

console = Console()

@dataclass
class Config:
    """Application configuration with sensible defaults."""
    
    # Data paths
    data_dir: Path = field(default_factory=lambda: Path("data/01_raw"))
    output_dir: Path = field(default_factory=lambda: Path("output"))
    schema_dir: Path = field(default_factory=lambda: Path("data/01_raw"))
    
    # Sampling configuration
    sample_size: int = 1000
    enable_sampling: bool = True
    small_dataset_mode: bool = False
    
    # Database configuration
    default_db_format: str = "sqlite"  # "sqlite", "duckdb", "duckdb_parquet"
    
    # Logging configuration
    log_level: str = "INFO"
    log_file: Optional[str] = None
    verbose: bool = False
    
    # Performance configuration
    batch_size: int = 1000
    max_workers: Optional[int] = None
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            data_dir=Path(os.getenv("SCENE_DATA_DIR", "data/01_raw")),
            output_dir=Path(os.getenv("SCENE_OUTPUT_DIR", "output")),
            schema_dir=Path(os.getenv("SCENE_SCHEMA_DIR", "data/01_raw")),
            sample_size=int(os.getenv("SCENE_SAMPLE_SIZE", "1000")),
            enable_sampling=os.getenv("SCENE_ENABLE_SAMPLING", "true").lower() == "true",
            small_dataset_mode=os.getenv("SCENE_SMALL_DATASET_MODE", "false").lower() == "true",
            default_db_format=os.getenv("SCENE_DB_FORMAT", "sqlite"),
            log_level=os.getenv("SCENE_LOG_LEVEL", "INFO"),
            log_file=os.getenv("SCENE_LOG_FILE"),
            verbose=os.getenv("SCENE_VERBOSE", "false").lower() == "true",
            batch_size=int(os.getenv("SCENE_BATCH_SIZE", "1000")),
            max_workers=int(os.getenv("SCENE_MAX_WORKERS", "0")) or None,
        )
    
    def validate(self) -> None:
        """Validate configuration values."""
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {self.data_dir}")
        
        if not self.schema_dir.exists():
            raise ValueError(f"Schema directory does not exist: {self.schema_dir}")
        
        if self.sample_size <= 0:
            raise ValueError(f"Sample size must be positive: {self.sample_size}")
        
        if self.batch_size <= 0:
            raise ValueError(f"Batch size must be positive: {self.batch_size}")
        
        if self.max_workers is not None and self.max_workers <= 0:
            raise ValueError(f"Max workers must be positive: {self.max_workers}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "data_dir": str(self.data_dir),
            "output_dir": str(self.output_dir),
            "schema_dir": str(self.schema_dir),
            "sample_size": self.sample_size,
            "enable_sampling": self.enable_sampling,
            "small_dataset_mode": self.small_dataset_mode,
            "default_db_format": self.default_db_format,
            "log_level": self.log_level,
            "log_file": self.log_file,
            "verbose": self.verbose,
            "batch_size": self.batch_size,
            "max_workers": self.max_workers,
        }
    
    def print_summary(self) -> None:
        """Print configuration summary."""
        console.print("[bold blue]Configuration Summary:[/bold blue]")
        console.print(f"[dim]Data Directory:[/dim] {self.data_dir}")
        console.print(f"[dim]Output Directory:[/dim] {self.output_dir}")
        console.print(f"[dim]Sample Size:[/dim] {self.sample_size:,}")
        console.print(f"[dim]Sampling Enabled:[/dim] {self.enable_sampling}")
        console.print(f"[dim]Small Dataset Mode:[/dim] {self.small_dataset_mode}")
        console.print(f"[dim]Default DB Format:[/dim] {self.default_db_format}")
        console.print(f"[dim]Log Level:[/dim] {self.log_level}")
        console.print(f"[dim]Verbose:[/dim] {self.verbose}")


# Global configuration instance
config = Config.from_env() 