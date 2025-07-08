#!/usr/bin/env python3
"""
Pytest configuration and fixtures for SceneUnderstanding tests.
"""

import pytest
import tempfile
import json
from pathlib import Path
from typing import Dict, List, Any, Generator
from unittest.mock import Mock, patch

from src.scene_understanding.types import (
    Category, License, Image, Attribute, Annotation, 
    CategoryDict, LicenseDict, ImageDict, AttributeDict, AnnotationDict
)
from src.scene_understanding.config import Config


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_categories() -> List[Category]:
    """Sample categories for testing."""
    return [
        Category(id=1, name="shirt", supercategory="top", level=1, taxonomy_id=1001),
        Category(id=2, name="pants", supercategory="bottom", level=1, taxonomy_id=1002),
        Category(id=3, name="dress", supercategory="dress", level=1, taxonomy_id=1003),
    ]


@pytest.fixture
def sample_licenses() -> List[License]:
    """Sample licenses for testing."""
    return [
        License(id=1, name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/"),
        License(id=2, name="CC BY-SA 4.0", url="https://creativecommons.org/licenses/by-sa/4.0/"),
    ]


@pytest.fixture
def sample_images() -> List[Image]:
    """Sample images for testing."""
    return [
        Image(id=1, license=1, file_name="image1.jpg", height=480, width=640),
        Image(id=2, license=1, file_name="image2.jpg", height=720, width=1280),
        Image(id=3, license=2, file_name="image3.jpg", height=360, width=640),
    ]


@pytest.fixture
def sample_annotations() -> List[Annotation]:
    """Sample annotations for testing."""
    return [
        Annotation(
            id=1, image_id=1, category_id=1, area=1000.0,
            bbox=[10, 20, 50, 60], iscrowd=0, attributes=[1, 2]
        ),
        Annotation(
            id=2, image_id=1, category_id=2, area=2000.0,
            bbox=[100, 150, 80, 90], iscrowd=0, attributes=[3]
        ),
        Annotation(
            id=3, image_id=2, category_id=1, area=1500.0,
            bbox=[30, 40, 70, 80], iscrowd=0
        ),
    ]


@pytest.fixture
def sample_attributes() -> List[Attribute]:
    """Sample attributes for testing."""
    return [
        Attribute(id=1, name="color", supercategory="appearance"),
        Attribute(id=2, name="pattern", supercategory="appearance"),
        Attribute(id=3, name="material", supercategory="texture"),
    ]


@pytest.fixture
def sample_json_data(
    sample_categories: List[Category],
    sample_licenses: List[License],
    sample_images: List[Image],
    sample_annotations: List[Annotation],
    sample_attributes: List[Attribute]
) -> Dict[str, Any]:
    """Sample JSON data structure for testing."""
    return {
        "info": {
            "description": "Fashionpedia test dataset",
            "version": "1.0",
            "year": 2024,
            "contributor": "Test"
        },
        "licenses": [
            {"id": l.id, "name": l.name, "url": l.url}
            for l in sample_licenses
        ],
        "categories": [
            {"id": c.id, "name": c.name, "supercategory": c.supercategory, 
             "level": c.level, "taxonomy_id": c.taxonomy_id}
            for c in sample_categories
        ],
        "attributes": [
            {"id": a.id, "name": a.name, "supercategory": a.supercategory}
            for a in sample_attributes
        ],
        "images": [
            {"id": img.id, "license": img.license, "file_name": img.file_name,
             "height": img.height, "width": img.width}
            for img in sample_images
        ],
        "annotations": [
            {"id": ann.id, "image_id": ann.image_id, "category_id": ann.category_id,
             "area": ann.area, "bbox": ann.bbox, "iscrowd": ann.iscrowd,
             "attributes": ann.attributes}
            for ann in sample_annotations
        ]
    }


@pytest.fixture
def sample_json_file(temp_dir: Path, sample_json_data: Dict[str, Any]) -> Path:
    """Create a sample JSON file for testing."""
    json_file = temp_dir / "sample_data.json"
    with open(json_file, 'w') as f:
        json.dump(sample_json_data, f)
    return json_file


@pytest.fixture
def test_config() -> Config:
    """Test configuration."""
    return Config(
        data_dir=Path("tests/data"),
        output_dir=Path("tests/output"),
        sample_size=100,
        enable_sampling=True,
        small_dataset_mode=False,
        verbose=True
    )


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch('src.scene_understanding.utils.logging.logger') as mock:
        yield mock


@pytest.fixture
def mock_console():
    """Mock rich console for testing."""
    with patch('src.scene_understanding.utils.logging.console') as mock:
        yield mock


class MockDatabase:
    """Mock database for testing."""
    
    def __init__(self):
        self.tables = {}
        self.executed_queries = []
    
    def execute(self, query: str, params: tuple = None) -> Mock:
        """Mock execute method."""
        self.executed_queries.append((query, params))
        return Mock()
    
    def commit(self) -> None:
        """Mock commit method."""
        pass
    
    def close(self) -> None:
        """Mock close method."""
        pass


@pytest.fixture
def mock_db():
    """Mock database connection."""
    return MockDatabase()


# Test markers
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    ) 