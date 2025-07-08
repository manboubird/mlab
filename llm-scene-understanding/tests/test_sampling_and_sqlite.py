#!/usr/bin/env python3
"""
Test script for sampling functionality and SQLite support
"""

import json
import tempfile
from pathlib import Path
from src.scene_understanding.loader_factory import FashionpediaLoaderFactory, LoadingStrategy

def create_test_data():
    """Create a small test dataset to verify sampling functionality"""
    test_data = {
        "info": {
            "year": 2020,
            "version": "1.0",
            "description": "Test Fashionpedia dataset",
            "contributor": "Test",
            "url": "https://test.com",
            "date_created": "2020-01-01"
        },
        "licenses": [
            {"id": 1, "name": "CC BY", "url": "https://creativecommons.org/licenses/by/4.0/"},
            {"id": 2, "name": "CC BY-SA", "url": "https://creativecommons.org/licenses/by-sa/4.0/"}
        ],
        "categories": [
            {"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"},
            {"id": 2, "name": "pants", "supercategory": "lower_body", "level": 1, "taxonomy_id": "cat_002"},
            {"id": 3, "name": "dress", "supercategory": "full_body", "level": 1, "taxonomy_id": "cat_003"}
        ],
        "attributes": [
            {"id": 1, "name": "color", "supercategory": "visual", "level": 1, "taxonomy_id": "attr_001"},
            {"id": 2, "name": "pattern", "supercategory": "visual", "level": 1, "taxonomy_id": "attr_002"},
            {"id": 3, "name": "material", "supercategory": "tactile", "level": 1, "taxonomy_id": "attr_003"}
        ],
        "images": [],
        "annotations": []
    }
    
    # Create 15,000 images (more than 10,000 to trigger sampling)
    for i in range(15000):
        test_data["images"].append({
            "id": i + 1,
            "width": 800,
            "height": 600,
            "file_name": f"image_{i+1:05d}.jpg",
            "license": 1 if i % 2 == 0 else 2,
            "time_captured": "2020-01-01",
            "original_url": f"https://test.com/image_{i+1}.jpg",
            "isstatic": True,
            "kaggle_id": f"kaggle_{i+1}"
        })
    
    # Create 20,000 annotations (more than 10,000 to trigger sampling)
    for i in range(20000):
        image_id = (i % 15000) + 1  # Distribute across images
        category_id = (i % 3) + 1   # Distribute across categories
        test_data["annotations"].append({
            "id": i + 1,
            "image_id": image_id,
            "category_id": category_id,
            "bbox": [100, 100, 200, 300],
            "area": 60000,
            "iscrowd": False,
            "attribute_ids": [(i % 3) + 1],
            "segmentation": [[100, 100, 300, 100, 300, 400, 100, 400]]
        })
    
    return test_data

def create_test_schema():
    """Create a test SQL schema"""
    return """
    CREATE TABLE IF NOT EXISTS info (
        year INTEGER,
        version VARCHAR,
        description VARCHAR,
        contributor VARCHAR,
        url VARCHAR,
        date_created VARCHAR
    );

    CREATE TABLE IF NOT EXISTS licenses (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        url VARCHAR
    );

    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        supercategory VARCHAR,
        level INTEGER,
        taxonomy_id VARCHAR
    );

    CREATE TABLE IF NOT EXISTS attributes (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        supercategory VARCHAR,
        level INTEGER,
        taxonomy_id VARCHAR
    );

    CREATE TABLE IF NOT EXISTS images (
        id INTEGER PRIMARY KEY,
        width INTEGER,
        height INTEGER,
        file_name VARCHAR,
        license INTEGER,
        time_captured VARCHAR,
        original_url VARCHAR,
        isstatic BOOLEAN,
        kaggle_id VARCHAR,
        FOREIGN KEY (license) REFERENCES licenses(id)
    );

    CREATE TABLE IF NOT EXISTS annotations (
        id INTEGER PRIMARY KEY,
        image_id INTEGER,
        category_id INTEGER,
        bbox_x DOUBLE,
        bbox_y DOUBLE,
        bbox_width DOUBLE,
        bbox_height DOUBLE,
        area DOUBLE,
        iscrowd BOOLEAN,
        FOREIGN KEY (image_id) REFERENCES images(id),
        FOREIGN KEY (category_id) REFERENCES categories(id)
    );

    CREATE TABLE IF NOT EXISTS annotation_attributes (
        annotation_id INTEGER,
        attribute_id INTEGER,
        PRIMARY KEY (annotation_id, attribute_id),
        FOREIGN KEY (annotation_id) REFERENCES annotations(id),
        FOREIGN KEY (attribute_id) REFERENCES attributes(id)
    );

    CREATE TABLE IF NOT EXISTS segmentations (
        annotation_id INTEGER,
        polygon_index INTEGER,
        point_index INTEGER,
        x_coord DOUBLE,
        y_coord DOUBLE,
        PRIMARY KEY (annotation_id, polygon_index, point_index),
        FOREIGN KEY (annotation_id) REFERENCES annotations(id)
    );
    """

def test_sampling_functionality():
    """Test sampling functionality with different strategies"""
    print("Testing sampling functionality...")
    
    # Create test data
    test_data = create_test_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save test data
        json_file = temp_path / "test_data.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Save schema
        schema_file = temp_path / "test_schema.sql"
        with open(schema_file, 'w') as f:
            f.write(create_test_schema())
        
        # Test different strategies with sampling
        strategies = [
            LoadingStrategy.DIRECT_JSON,
            LoadingStrategy.PARQUET_CONVERSION,
            LoadingStrategy.SQLITE_DIRECT
        ]
        
        for strategy in strategies:
            print(f"\n--- Testing {strategy.value} with sampling ---")
            
            if strategy == LoadingStrategy.SQLITE_DIRECT:
                db_path = temp_path / f"test_{strategy.value}.db"
            else:
                db_path = temp_path / f"test_{strategy.value}.ddb"
            
            try:
                # Create loader with sampling enabled
                with FashionpediaLoaderFactory.create_loader(
                    strategy, db_path, sample_size=1000, enable_sampling=True
                ) as loader:
                    # Create schema
                    loader.create_schema(schema_file)
                    
                    # Load data (should trigger sampling)
                    loader.load_json_data(json_file)
                    
                    # Get statistics
                    stats = loader.get_database_stats()
                    
                    print(f"✓ {strategy.value} - Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                    
                    # Verify sampling worked (should be around 1000, not 15,000/20,000)
                    if stats['total_images'] < 5000 and stats['total_annotations'] < 5000:
                        print(f"  ✓ Sampling working correctly")
                    else:
                        print(f"  ✗ Sampling may not be working (got {stats['total_images']:,} images)")
                        
            except Exception as e:
                print(f"✗ {strategy.value} failed: {e}")

def test_full_loading():
    """Test full loading without sampling"""
    print("\nTesting full loading without sampling...")
    
    # Create test data
    test_data = create_test_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save test data
        json_file = temp_path / "test_data.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Save schema
        schema_file = temp_path / "test_schema.sql"
        with open(schema_file, 'w') as f:
            f.write(create_test_schema())
        
        # Test DuckDB strategy without sampling
        db_path = temp_path / "test_full.ddb"
        
        try:
            with FashionpediaLoaderFactory.create_loader(
                LoadingStrategy.DIRECT_JSON, db_path, enable_sampling=False
            ) as loader:
                # Create schema
                loader.create_schema(schema_file)
                
                # Load data (should load everything)
                loader.load_json_data(json_file)
                
                # Get statistics
                stats = loader.get_database_stats()
                
                print(f"✓ Full loading - Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                
                # Verify full loading worked
                if stats['total_images'] == 15000 and stats['total_annotations'] == 20000:
                    print(f"  ✓ Full loading working correctly")
                else:
                    print(f"  ✗ Full loading may not be working (expected 15,000/20,000, got {stats['total_images']:,}/{stats['total_annotations']:,})")
                    
        except Exception as e:
            print(f"✗ Full loading failed: {e}")

def test_sqlite_specific_features():
    """Test SQLite-specific features"""
    print("\nTesting SQLite-specific features...")
    
    # Create smaller test data for SQLite
    test_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Test"},
        "licenses": [{"id": 1, "name": "CC BY", "url": "https://test.com"}],
        "categories": [{"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"}],
        "attributes": [{"id": 1, "name": "color", "supercategory": "visual", "level": 1, "taxonomy_id": "attr_001"}],
        "images": [
            {"id": 1, "width": 800, "height": 600, "file_name": "test.jpg", "license": 1, "time_captured": "2020-01-01", "original_url": "https://test.com", "isstatic": True, "kaggle_id": "test"}
        ],
        "annotations": [
            {"id": 1, "image_id": 1, "category_id": 1, "bbox": [100, 100, 200, 300], "area": 60000, "iscrowd": False, "attribute_ids": [1], "segmentation": [[100, 100, 300, 100, 300, 400, 100, 400]]}
        ]
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save test data
        json_file = temp_path / "test_data.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Save schema
        schema_file = temp_path / "test_schema.sql"
        with open(schema_file, 'w') as f:
            f.write(create_test_schema())
        
        # Test SQLite
        db_path = temp_path / "test_sqlite.db"
        
        try:
            with FashionpediaLoaderFactory.create_loader(
                LoadingStrategy.SQLITE_DIRECT, db_path
            ) as loader:
                # Create schema
                loader.create_schema(schema_file)
                
                # Load data
                loader.load_json_data(json_file)
                
                # Get statistics
                stats = loader.get_database_stats()
                
                print(f"✓ SQLite - Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                
                # Test sample queries
                print("  Running sample queries...")
                loader.run_sample_queries()
                
                # Test CSV export
                csv_dir = temp_path / "csv_export"
                loader.export_to_csv(csv_dir)
                
                if csv_dir.exists() and any(csv_dir.iterdir()):
                    print(f"  ✓ CSV export working")
                else:
                    print(f"  ✗ CSV export failed")
                    
        except Exception as e:
            print(f"✗ SQLite test failed: {e}")

if __name__ == "__main__":
    print("=== Testing Sampling and SQLite Functionality ===\n")
    
    # Test sampling functionality
    test_sampling_functionality()
    
    # Test full loading
    test_full_loading()
    
    # Test SQLite-specific features
    test_sqlite_specific_features()
    
    print("\n=== Testing Complete ===") 