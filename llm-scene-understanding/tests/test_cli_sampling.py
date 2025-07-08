#!/usr/bin/env python3
"""
Simple test for CLI sampling functionality
"""

import json
import tempfile
from pathlib import Path
from src.scene_understanding.loader_factory import FashionpediaLoaderFactory, LoadingStrategy

def test_cli_sampling():
    """Test that sampling works correctly with CLI parameters"""
    print("Testing CLI sampling functionality...")
    
    # Create small test data
    test_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Test"},
        "licenses": [{"id": 1, "name": "CC BY", "url": "https://test.com"}],
        "categories": [{"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"}],
        "attributes": [{"id": 1, "name": "color", "supercategory": "visual", "level": 1, "taxonomy_id": "attr_001"}],
        "images": [],
        "annotations": []
    }
    
    # Create 12,000 images (more than 10,000 to trigger sampling)
    for i in range(12000):
        test_data["images"].append({
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
    
    # Create 15,000 annotations (more than 10,000 to trigger sampling)
    for i in range(15000):
        image_id = (i % 12000) + 1
        test_data["annotations"].append({
            "id": i + 1,
            "image_id": image_id,
            "category_id": 1,
            "bbox": [100, 100, 200, 300],
            "area": 60000,
            "iscrowd": False,
            "attribute_ids": [1],
            "segmentation": [[100, 100, 300, 100, 300, 400, 100, 400]]
        })
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save test data
        json_file = temp_path / "test_data.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Create simple schema
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
        
        # Test different strategies with CLI-like parameters
        test_cases = [
            {
                "strategy": LoadingStrategy.DIRECT_JSON,
                "sample_size": 1000,
                "enable_sampling": True,
                "name": "DuckDB with sampling"
            },
            {
                "strategy": LoadingStrategy.SQLITE_DIRECT,
                "sample_size": 500,
                "enable_sampling": True,
                "name": "SQLite with sampling"
            },
            {
                "strategy": LoadingStrategy.DIRECT_JSON,
                "sample_size": None,
                "enable_sampling": False,
                "name": "DuckDB without sampling"
            }
        ]
        
        for test_case in test_cases:
            print(f"\n--- Testing {test_case['name']} ---")
            
            if test_case['strategy'] == LoadingStrategy.SQLITE_DIRECT:
                db_path = temp_path / f"test_{test_case['strategy'].value}.db"
            else:
                db_path = temp_path / f"test_{test_case['strategy'].value}.ddb"
            
            try:
                with FashionpediaLoaderFactory.create_loader(
                    test_case['strategy'], 
                    db_path, 
                    sample_size=test_case['sample_size'],
                    enable_sampling=test_case['enable_sampling']
                ) as loader:
                    # Create schema
                    loader.create_schema(schema_file)
                    
                    # Load data
                    loader.load_json_data(json_file)
                    
                    # Get statistics
                    stats = loader.get_database_stats()
                    
                    print(f"✓ Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                    
                    # Verify expected behavior
                    if test_case['enable_sampling']:
                        if stats['total_images'] < 5000 and stats['total_annotations'] < 5000:
                            print(f"  ✓ Sampling working correctly")
                        else:
                            print(f"  ✗ Sampling may not be working")
                    else:
                        if stats['total_images'] == 12000 and stats['total_annotations'] == 15000:
                            print(f"  ✓ Full loading working correctly")
                        else:
                            print(f"  ✗ Full loading may not be working")
                            
            except Exception as e:
                print(f"✗ Failed: {e}")

if __name__ == "__main__":
    test_cli_sampling() 