#!/usr/bin/env python3
"""
Loader Factory for Fashionpedia Data
Provides strategy pattern for choosing between different loading approaches
"""

from enum import Enum
from pathlib import Path
from typing import Protocol, Optional
from .duckdb_loader import FashionpediaDuckDBLoader
from .parquet_loader import FashionpediaParquetLoader
from .sqlite_loader import FashionpediaSQLiteLoader

class LoadingStrategy(Enum):
    """Available loading strategies"""
    DIRECT_JSON = "direct_json"
    PARQUET_CONVERSION = "parquet_conversion"
    SQLITE_DIRECT = "sqlite_direct"

class FashionpediaLoader(Protocol):
    """Protocol for Fashionpedia data loaders"""
    
    def __enter__(self):
        """Context manager entry"""
        ...
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        ...
        
    def create_schema(self, schema_file: Path):
        """Create database schema"""
        ...
        
    def load_json_data(self, json_file: Path):
        """Load JSON data"""
        ...
        
    def get_database_stats(self):
        """Get database statistics"""
        ...
        
    def run_sample_queries(self):
        """Run sample queries"""
        ...

class FashionpediaLoaderFactory:
    """
    Factory for creating Fashionpedia data loaders
    """
    
    @staticmethod
    def create_loader(strategy: LoadingStrategy, db_path: Path, 
                     sample_size: Optional[int] = None, 
                     enable_sampling: bool = False,
                     small_dataset_mode: bool = False,
                     **kwargs):
        """
        Create a loader based on the specified strategy
        
        Args:
            strategy: Loading strategy to use
            db_path: Path to database file
            sample_size: Number of samples to use when sampling is enabled
            enable_sampling: Whether to enable sampling mode
            small_dataset_mode: Use reduced sampling numbers for faster processing
            **kwargs: Additional arguments for specific loaders
            
        Returns:
            FashionpediaLoader instance
        """
        if strategy == LoadingStrategy.DIRECT_JSON:
            return FashionpediaDuckDBLoader(
                db_path, 
                sample_size=sample_size, 
                enable_sampling=enable_sampling,
                small_dataset_mode=small_dataset_mode
            )
        elif strategy == LoadingStrategy.PARQUET_CONVERSION:
            temp_dir = kwargs.get('temp_dir')
            no_cleanup = kwargs.get('no_cleanup', False)
            return FashionpediaParquetLoader(
                db_path, 
                temp_dir=temp_dir, 
                sample_size=sample_size, 
                enable_sampling=enable_sampling,
                small_dataset_mode=small_dataset_mode,
                no_cleanup=no_cleanup
            )
        elif strategy == LoadingStrategy.SQLITE_DIRECT:
            return FashionpediaSQLiteLoader(
                db_path, 
                sample_size=sample_size, 
                enable_sampling=enable_sampling,
                small_dataset_mode=small_dataset_mode
            )
        else:
            raise ValueError(f"Unknown loading strategy: {strategy}")
    
    @staticmethod
    def get_strategy_description(strategy: LoadingStrategy) -> str:
        """
        Get description of the loading strategy
        
        Args:
            strategy: Loading strategy
            
        Returns:
            Description of the strategy
        """
        descriptions = {
            LoadingStrategy.DIRECT_JSON: "Direct JSON loading using INSERT statements",
            LoadingStrategy.PARQUET_CONVERSION: "JSON to Parquet conversion for optimal performance",
            LoadingStrategy.SQLITE_DIRECT: "Direct JSON loading using INSERT statements"
        }
        return descriptions.get(strategy, "Unknown strategy")
    
    @staticmethod
    def get_strategy_benefits(strategy: LoadingStrategy) -> list:
        """
        Get benefits of the loading strategy
        
        Args:
            strategy: Loading strategy
            
        Returns:
            List of benefits
        """
        benefits = {
            LoadingStrategy.DIRECT_JSON: [
                "Simple and straightforward",
                "No temporary files",
                "Immediate data availability"
            ],
            LoadingStrategy.PARQUET_CONVERSION: [
                "Optimal performance for large datasets",
                "Efficient storage format",
                "Fast query performance"
            ],
            LoadingStrategy.SQLITE_DIRECT: [
                "Simple and straightforward",
                "No temporary files",
                "Widely compatible SQLite format"
            ]
        }
        return benefits.get(strategy, [])
    
    @staticmethod
    def get_strategy_recommendations() -> dict:
        """
        Get recommendations for when to use each strategy
        
        Returns:
            Dictionary with recommendations
        """
        return {
            LoadingStrategy.DIRECT_JSON: {
                "best_for": [
                    "Small to medium datasets (< 1GB)",
                    "One-time data loading",
                    "Development and testing",
                    "When disk space is limited",
                    "Analytical workloads",
                    "When DuckDB performance is needed"
                ],
                "considerations": [
                    "Slower for large datasets",
                    "Higher storage requirements",
                    "May be slower for complex queries"
                ]
            },
            LoadingStrategy.PARQUET_CONVERSION: {
                "best_for": [
                    "Large datasets (> 1GB)",
                    "Frequent querying",
                    "Production environments",
                    "Analytical workloads",
                    "When performance is critical",
                    "When storage space is limited"
                ],
                "considerations": [
                    "Requires temporary disk space",
                    "Two-step process (JSON → Parquet → DuckDB)",
                    "Initial conversion overhead"
                ]
            },
            LoadingStrategy.SQLITE_DIRECT: {
                "best_for": [
                    "Small to medium datasets",
                    "Portable applications",
                    "When SQLite compatibility is needed",
                    "Transactional workloads",
                    "Cross-platform deployment",
                    "When external dependencies should be minimized"
                ],
                "considerations": [
                    "Slower than DuckDB for analytical queries",
                    "Limited concurrent access",
                    "May not handle very large datasets as well"
                ]
            }
        } 