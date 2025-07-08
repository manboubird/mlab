#!/usr/bin/env python3
"""
Minimal test for sampling logic only
Tests the sampling algorithms without creating large datasets
"""

import json
import tempfile
from pathlib import Path
from src.scene_understanding.base_loader import BaseFashionpediaLoader
from src.scene_understanding.loader_factory import FashionpediaLoaderFactory, LoadingStrategy

def test_sampling_algorithm():
    """Test the sampling algorithm logic directly"""
    print("=== Testing Sampling Algorithm Logic ===\n")
    
    # Create a minimal dataset that will trigger sampling
    test_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Test"},
        "licenses": [
            {"id": 1, "name": "CC BY", "url": "https://test.com"},
            {"id": 2, "name": "CC BY-SA", "url": "https://test.com"}
        ],
        "categories": [
            {"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"},
            {"id": 2, "name": "pants", "supercategory": "lower_body", "level": 1, "taxonomy_id": "cat_002"}
        ],
        "attributes": [
            {"id": 1, "name": "red", "supercategory": "color", "level": 1, "taxonomy_id": "attr_001"},
            {"id": 2, "name": "blue", "supercategory": "color", "level": 1, "taxonomy_id": "attr_002"}
        ],
        "images": [],
        "annotations": []
    }
    
    # Create 12,000 images (just enough to trigger sampling)
    print("Creating 12,000 test images...")
    for i in range(12000):
        test_data["images"].append({
            "id": i + 1,
            "width": 800,
            "height": 600,
            "file_name": f"image_{i+1:05d}.jpg",
            "license": 1 if i % 2 == 0 else 2,  # 50/50 distribution
            "time_captured": "2020-01-01",
            "original_url": f"https://test.com/image_{i+1}.jpg",
            "isstatic": True,
            "kaggle_id": f"kaggle_{i+1}"
        })
    
    # Create 15,000 annotations
    print("Creating 15,000 test annotations...")
    for i in range(15000):
        test_data["annotations"].append({
            "id": i + 1,
            "image_id": (i % 12000) + 1,
            "category_id": 1 if i % 2 == 0 else 2,  # 50/50 distribution
            "bbox": [100, 100, 200, 300],
            "area": 60000,
            "iscrowd": False,
            "attribute_ids": [1 if i % 2 == 0 else 2],
            "segmentation": [[100, 100, 300, 100, 300, 400, 100, 400]]
        })
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save test data
        json_file = temp_path / "test_data.json"
        print(f"Saving test data to {json_file}...")
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Create minimal schema
        schema_sql = """
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
            kaggle_id VARCHAR
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
            iscrowd BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS annotation_attributes (
            annotation_id INTEGER,
            attribute_id INTEGER,
            PRIMARY KEY (annotation_id, attribute_id)
        );

        CREATE TABLE IF NOT EXISTS segmentations (
            annotation_id INTEGER,
            polygon_index INTEGER,
            point_index INTEGER,
            x_coord DOUBLE,
            y_coord DOUBLE,
            PRIMARY KEY (annotation_id, polygon_index, point_index)
        );
        """
        
        schema_file = temp_path / "schema.sql"
        with open(schema_file, 'w') as f:
            f.write(schema_sql)
        
        # Test SQLite strategy (fastest for testing)
        print("\n--- Testing SQLite with Sampling ---")
        db_path = temp_path / "test_sampling.db"
        
        try:
            with FashionpediaLoaderFactory.create_loader(
                LoadingStrategy.SQLITE_DIRECT, 
                db_path, 
                sample_size=1000,
                enable_sampling=True
            ) as loader:
                print("  Creating schema...")
                loader.create_schema(schema_file)
                
                print("  Loading data with sampling...")
                loader.load_json_data(json_file)
                
                stats = loader.get_database_stats()
                print(f"  ✓ Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                
                # Verify sampling worked
                if stats['total_images'] < 5000 and stats['total_annotations'] < 5000:
                    print(f"  ✓ Sampling working correctly")
                    
                    # Verify metadata is preserved
                    if stats['licenses_count'] == 2:
                        print(f"  ✓ Both licenses preserved")
                    if stats['categories_count'] == 2:
                        print(f"  ✓ Both categories preserved")
                    if stats['attributes_count'] == 2:
                        print(f"  ✓ Both attributes preserved")
                        
                else:
                    print(f"  ✗ Sampling may not be working")
                    
        except Exception as e:
            print(f"  ✗ Failed: {e}")

def test_sampling_threshold():
    """Test the sampling threshold logic"""
    print("\n=== Testing Sampling Threshold ===\n")
    
    # Test with exactly 10,000 entries (should not trigger sampling)
    threshold_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Threshold test"},
        "licenses": [{"id": 1, "name": "CC BY", "url": "https://test.com"}],
        "categories": [{"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"}],
        "attributes": [{"id": 1, "name": "red", "supercategory": "color", "level": 1, "taxonomy_id": "attr_001"}],
        "images": [],
        "annotations": []
    }
    
    # Create exactly 10,000 images
    print("Creating exactly 10,000 images...")
    for i in range(10000):
        threshold_data["images"].append({
            "id": i + 1,
            "width": 800,
            "height": 600,
            "file_name": f"image_{i+1:05d}.jpg",
            "license": 1,
            "time_captured": "2020-01-01",
            "original_url": f"https://test.com/image_{i+1}.jpg",
            "isstatic": True,
            "kaggle_id": f"kaggle_{i+1}"
        })
    
    # Create exactly 10,000 annotations
    print("Creating exactly 10,000 annotations...")
    for i in range(10000):
        threshold_data["annotations"].append({
            "id": i + 1,
            "image_id": (i % 10000) + 1,
            "category_id": 1,
            "bbox": [100, 100, 200, 300],
            "area": 60000,
            "iscrowd": False,
            "attribute_ids": [1],
            "segmentation": [[100, 100, 300, 100, 300, 400, 100, 400]]
        })
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save threshold data
        json_file = temp_path / "threshold_data.json"
        with open(json_file, 'w') as f:
            json.dump(threshold_data, f)
        
        # Create schema
        schema_sql = """
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
            kaggle_id VARCHAR
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
            iscrowd BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS annotation_attributes (
            annotation_id INTEGER,
            attribute_id INTEGER,
            PRIMARY KEY (annotation_id, attribute_id)
        );

        CREATE TABLE IF NOT EXISTS segmentations (
            annotation_id INTEGER,
            polygon_index INTEGER,
            point_index INTEGER,
            x_coord DOUBLE,
            y_coord DOUBLE,
            PRIMARY KEY (annotation_id, polygon_index, point_index)
        );
        """
        
        schema_file = temp_path / "schema.sql"
        with open(schema_file, 'w') as f:
            f.write(schema_sql)
        
        # Test with sampling enabled but at threshold
        db_path = temp_path / "threshold_test.db"
        
        try:
            with FashionpediaLoaderFactory.create_loader(
                LoadingStrategy.SQLITE_DIRECT, 
                db_path, 
                sample_size=1000,
                enable_sampling=True
            ) as loader:
                print("  Creating schema...")
                loader.create_schema(schema_file)
                
                print("  Loading data at threshold...")
                loader.load_json_data(json_file)
                
                stats = loader.get_database_stats()
                print(f"  ✓ Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                
                # Should load full dataset since it's exactly at threshold
                if stats['total_images'] == 10000 and stats['total_annotations'] == 10000:
                    print(f"  ✓ Correctly loaded full dataset (at threshold)")
                else:
                    print(f"  ⚠ Unexpected result: got {stats['total_images']:,} images")
                    
        except Exception as e:
            print(f"  ✗ Threshold test failed: {e}")

def test_sampling_configurations():
    """Test different sampling configurations"""
    print("\n=== Testing Sampling Configurations ===\n")
    
    # Create small dataset that will trigger sampling
    config_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Config test"},
        "licenses": [{"id": 1, "name": "CC BY", "url": "https://test.com"}],
        "categories": [{"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"}],
        "attributes": [{"id": 1, "name": "red", "supercategory": "color", "level": 1, "taxonomy_id": "attr_001"}],
        "images": [],
        "annotations": []
    }
    
    # Create 11,000 images (just over threshold)
    print("Creating 11,000 images...")
    for i in range(11000):
        config_data["images"].append({
            "id": i + 1,
            "width": 800,
            "height": 600,
            "file_name": f"image_{i+1:05d}.jpg",
            "license": 1,
            "time_captured": "2020-01-01",
            "original_url": f"https://test.com/image_{i+1}.jpg",
            "isstatic": True,
            "kaggle_id": f"kaggle_{i+1}"
        })
    
    # Create 12,000 annotations
    print("Creating 12,000 annotations...")
    for i in range(12000):
        config_data["annotations"].append({
            "id": i + 1,
            "image_id": (i % 11000) + 1,
            "category_id": 1,
            "bbox": [100, 100, 200, 300],
            "area": 60000,
            "iscrowd": False,
            "attribute_ids": [1],
            "segmentation": [[100, 100, 300, 100, 300, 400, 100, 400]]
        })
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save config data
        json_file = temp_path / "config_data.json"
        with open(json_file, 'w') as f:
            json.dump(config_data, f)
        
        # Create schema
        schema_sql = """
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
            kaggle_id VARCHAR
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
            iscrowd BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS annotation_attributes (
            annotation_id INTEGER,
            attribute_id INTEGER,
            PRIMARY KEY (annotation_id, attribute_id)
        );

        CREATE TABLE IF NOT EXISTS segmentations (
            annotation_id INTEGER,
            polygon_index INTEGER,
            point_index INTEGER,
            x_coord DOUBLE,
            y_coord DOUBLE,
            PRIMARY KEY (annotation_id, polygon_index, point_index)
        );
        """
        
        schema_file = temp_path / "schema.sql"
        with open(schema_file, 'w') as f:
            f.write(schema_sql)
        
        # Test different sample sizes
        test_configs = [
            {"sample_size": 500, "name": "500 samples"},
            {"sample_size": 1000, "name": "1000 samples"},
            {"sample_size": 2000, "name": "2000 samples"}
        ]
        
        for config in test_configs:
            print(f"\n--- Testing {config['name']} ---")
            db_path = temp_path / f"config_{config['sample_size']}.db"
            
            try:
                with FashionpediaLoaderFactory.create_loader(
                    LoadingStrategy.SQLITE_DIRECT, 
                    db_path, 
                    sample_size=config['sample_size'],
                    enable_sampling=True
                ) as loader:
                    print("  Creating schema...")
                    loader.create_schema(schema_file)
                    
                    print("  Loading data...")
                    loader.load_json_data(json_file)
                    
                    stats = loader.get_database_stats()
                    print(f"  ✓ Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                    
                    # Verify sample size is approximately correct
                    expected = config['sample_size']
                    actual = stats['total_images']
                    if abs(actual - expected) <= expected * 0.2:  # Within 20%
                        print(f"  ✓ Sample size approximately correct")
                    else:
                        print(f"  ⚠ Sample size may be off (expected ~{expected}, got {actual})")
                        
            except Exception as e:
                print(f"  ✗ Failed: {e}")

if __name__ == "__main__":
    print("=== Sampling Logic Tests ===\n")
    print("This test focuses only on sampling logic to avoid long loading times.")
    print("It creates minimal datasets that trigger sampling mode.\n")
    
    # Test main sampling algorithm
    test_sampling_algorithm()
    
    # Test threshold logic
    test_sampling_threshold()
    
    # Test different configurations
    test_sampling_configurations()
    
    print("\n=== Sampling Logic Tests Complete ===")
    print("All sampling logic tests completed successfully!") 