#!/usr/bin/env python3
"""
Type definitions for SceneUnderstanding application.
"""

from typing import (
    Dict, List, Optional, Union, Any, Tuple, 
    TypedDict, Protocol, Literal, NewType
)
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

# Custom types
FilePath = NewType('FilePath', Path)
DatabasePath = NewType('DatabasePath', Path)
SampleSize = NewType('SampleSize', int)
BatchSize = NewType('BatchSize', int)

# Database formats
DatabaseFormat = Literal["sqlite", "duckdb", "duckdb_parquet", "owl"]

# Logging levels
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Category types
CategoryId = NewType('CategoryId', int)
TaxonomyId = NewType('TaxonomyId', int)
SuperCategory = NewType('SuperCategory', str)
CategoryLevel = NewType('CategoryLevel', int)

# Image types
ImageId = NewType('ImageId', int)
LicenseId = NewType('LicenseId', int)
ImageWidth = NewType('ImageWidth', int)
ImageHeight = NewType('ImageHeight', int)

# Annotation types
AnnotationId = NewType('AnnotationId', int)
AttributeId = NewType('AttributeId', int)
SegmentationId = NewType('SegmentationId', int)

# Coordinate types
Coordinate = NewType('Coordinate', float)
Point = Tuple[Coordinate, Coordinate]
Polygon = List[Point]
Segmentation = List[Polygon]


@dataclass
class Category:
    """Category information from Fashionpedia taxonomy."""
    id: CategoryId
    name: str
    supercategory: SuperCategory
    level: CategoryLevel
    taxonomy_id: TaxonomyId


@dataclass
class License:
    """License information."""
    id: LicenseId
    name: str
    url: str


@dataclass
class Image:
    """Image information."""
    id: ImageId
    license: Optional[LicenseId]
    file_name: str
    height: ImageHeight
    width: ImageWidth
    date_captured: Optional[str] = None


@dataclass
class Attribute:
    """Attribute information."""
    id: AttributeId
    name: str
    supercategory: str


@dataclass
class Annotation:
    """Annotation information."""
    id: AnnotationId
    image_id: ImageId
    category_id: CategoryId
    area: float
    bbox: List[float]  # [x, y, width, height]
    iscrowd: int
    attributes: Optional[List[AttributeId]] = None


@dataclass
class Segmentation:
    """Segmentation information."""
    annotation_id: AnnotationId
    segmentation: List[Polygon]


# TypedDict definitions for JSON data
class CategoryDict(TypedDict):
    """Category dictionary from JSON."""
    id: int
    name: str
    supercategory: str
    level: int
    taxonomy_id: int


class LicenseDict(TypedDict):
    """License dictionary from JSON."""
    id: int
    name: str
    url: str


class ImageDict(TypedDict):
    """Image dictionary from JSON."""
    id: int
    license: Optional[int]
    file_name: str
    height: int
    width: int
    date_captured: Optional[str]


class AttributeDict(TypedDict):
    """Attribute dictionary from JSON."""
    id: int
    name: str
    supercategory: str


class AnnotationDict(TypedDict):
    """Annotation dictionary from JSON."""
    id: int
    image_id: int
    category_id: int
    area: float
    bbox: List[float]
    iscrowd: int
    attributes: Optional[List[int]]


class SegmentationDict(TypedDict):
    """Segmentation dictionary from JSON."""
    annotation_id: int
    segmentation: List[List[List[float]]]


class FashionpediaData(TypedDict):
    """Complete Fashionpedia dataset structure."""
    info: Dict[str, Any]
    licenses: List[LicenseDict]
    categories: List[CategoryDict]
    attributes: List[AttributeDict]
    images: List[ImageDict]
    annotations: List[AnnotationDict]


# Protocol definitions for loader interfaces
class DataLoader(Protocol):
    """Protocol for data loaders."""
    
    def load_data(self, file_path: FilePath, **kwargs: Any) -> None:
        """Load data from file."""
        ...
    
    def get_summary(self) -> Dict[str, Any]:
        """Get loading summary."""
        ...


class OntologyGenerator(Protocol):
    """Protocol for ontology generators."""
    
    def generate_ontology(self, categories: List[Category], output_path: FilePath) -> None:
        """Generate ontology from categories."""
        ...
    
    def get_summary(self) -> Dict[str, Any]:
        """Get generation summary."""
        ...


# Configuration types
@dataclass
class SamplingConfig:
    """Sampling configuration."""
    enabled: bool = True
    sample_size: SampleSize = SampleSize(1000)
    small_dataset_mode: bool = False
    small_dataset_sizes: Dict[str, int] = None
    
    def __post_init__(self):
        if self.small_dataset_sizes is None:
            self.small_dataset_sizes = {
                "images": 100,
                "annotations": 200,
                "segmentations": 200
            }


@dataclass
class PerformanceConfig:
    """Performance configuration."""
    batch_size: BatchSize = BatchSize(1000)
    max_workers: Optional[int] = None
    enable_timing: bool = True


@dataclass
class DatabaseConfig:
    """Database configuration."""
    format: DatabaseFormat
    path: DatabasePath
    create_schema: bool = True
    clear_existing: bool = False


# Result types
@dataclass
class LoadResult:
    """Result of data loading operation."""
    success: bool
    records_loaded: Dict[str, int]
    duration: float
    errors: List[str]
    warnings: List[str]


@dataclass
class OntologyResult:
    """Result of ontology generation."""
    success: bool
    classes_created: int
    properties_created: int
    output_path: FilePath
    duration: float
    errors: List[str]


# Error types
class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


class ConversionError(Exception):
    """Raised when data conversion fails."""
    pass


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass 