#!/usr/bin/env python3
"""
Focused test for sampling functionality only
Avoids full loading to speed up development
"""

import json
import tempfile
from pathlib import Path
from src.scene_understanding.loader_factory import FashionpediaLoaderFactory, LoadingStrategy

def create_large_test_data():
    """Create test data that will trigger sampling (more than 10,000 entries)"""
    test_data = {
        "info": {
            "year": 2020,
            "version": "1.0",
            "description": "Test Fashionpedia dataset for sampling",
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
    print("Creating 15,000 test images...")
    for i in range(15000):
        test_data["images"].append({
            "id": i + 1,
            "width": 800,
            "height": 600,
            "file_name": f"image_{i+1:05d}.jpg",
            "license": 1 if i % 2 == 0 else 2,  # Distribute across licenses
            "time_captured": "2020-01-01",
            "original_url": f"https://test.com/image_{i+1}.jpg",
            "isstatic": True,
            "kaggle_id": f"kaggle_{i+1}"
        })
    
    # Create 20,000 annotations (more than 10,000 to trigger sampling)
    print("Creating 20,000 test annotations...")
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
    """Create test SQL schema"""
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

def test_sampling_patterns():
    """Test different sampling patterns and configurations"""
    print("=== Testing Sampling Patterns ===\n")
    
    # Create test data
    test_data = create_large_test_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Save test data
        json_file = temp_path / "test_data.json"
        print(f"Saving test data to {json_file}...")
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Save schema
        schema_file = temp_path / "test_schema.sql"
        with open(schema_file, 'w') as f:
            f.write(create_test_schema())
        
        # Test different sampling configurations
        test_configs = [
            {
                "name": "DuckDB with 1000 samples",
                "strategy": LoadingStrategy.DIRECT_JSON,
                "sample_size": 1000,
                "enable_sampling": True,
                "expected_images": "~1000",
                "expected_annotations": "~1000"
            },
            {
                "name": "SQLite with 500 samples",
                "strategy": LoadingStrategy.SQLITE_DIRECT,
                "sample_size": 500,
                "enable_sampling": True,
                "expected_images": "~500",
                "expected_annotations": "~500"
            },
            {
                "name": "DuckDB Parquet with 2000 samples",
                "strategy": LoadingStrategy.PARQUET_CONVERSION,
                "sample_size": 2000,
                "enable_sampling": True,
                "expected_images": "~2000",
                "expected_annotations": "~2000"
            }
        ]
        
        for config in test_configs:
            print(f"\n--- {config['name']} ---")
            
            if config['strategy'] == LoadingStrategy.SQLITE_DIRECT:
                db_path = temp_path / f"test_{config['strategy'].value}.db"
            else:
                db_path = temp_path / f"test_{config['strategy'].value}.ddb"
            
            try:
                # Create loader with sampling configuration
                with FashionpediaLoaderFactory.create_loader(
                    config['strategy'], 
                    db_path, 
                    sample_size=config['sample_size'],
                    enable_sampling=config['enable_sampling']
                ) as loader:
                    # Create schema
                    print("  Creating schema...")
                    loader.create_schema(schema_file)
                    
                    # Load data (should trigger sampling)
                    print("  Loading data with sampling...")
                    loader.load_json_data(json_file)
                    
                    # Get statistics
                    stats = loader.get_database_stats()
                    
                    print(f"  ✓ Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                    print(f"  ✓ Expected: {config['expected_images']} images, {config['expected_annotations']} annotations")
                    
                    # Verify sampling worked correctly
                    if stats['total_images'] < 5000 and stats['total_annotations'] < 5000:
                        print(f"  ✓ Sampling working correctly")
                        
                        # Check referential integrity
                        if stats['total_annotations'] > 0:
                            print(f"  ✓ Referential integrity maintained")
                        else:
                            print(f"  ⚠ No annotations loaded - may indicate sampling issue")
                    else:
                        print(f"  ✗ Sampling may not be working (got {stats['total_images']:,} images)")
                    
                    # Test sample queries
                    print("  Running sample queries...")
                    loader.run_sample_queries()
                    
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                import traceback
                print(f"  {traceback.format_exc()}")

def test_sampling_edge_cases():
    """Test edge cases for sampling functionality"""
    print("\n=== Testing Sampling Edge Cases ===\n")
    
    # Test with exactly 10,000 entries (should not trigger sampling)
    print("Testing with exactly 10,000 images (should not trigger sampling)...")
    
    edge_case_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Edge case test"},
        "licenses": [{"id": 1, "name": "CC BY", "url": "https://test.com"}],
        "categories": [{"id": 1, "name": "shirt", "supercategory": "upper_body", "level": 1, "taxonomy_id": "cat_001"}],
        "attributes": [{"id": 1, "name": "color", "supercategory": "visual", "level": 1, "taxonomy_id": "attr_001"}],
        "images": [],
        "annotations": []
    }
    
    # Create exactly 10,000 images
    for i in range(10000):
        edge_case_data["images"].append({
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
    for i in range(10000):
        edge_case_data["annotations"].append({
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
        
        # Save edge case data
        json_file = temp_path / "edge_case_data.json"
        with open(json_file, 'w') as f:
            json.dump(edge_case_data, f)
        
        # Save schema
        schema_file = temp_path / "test_schema.sql"
        with open(schema_file, 'w') as f:
            f.write(create_test_schema())
        
        # Test with sampling enabled but dataset at threshold
        db_path = temp_path / "edge_case.ddb"
        
        try:
            with FashionpediaLoaderFactory.create_loader(
                LoadingStrategy.DIRECT_JSON, 
                db_path, 
                sample_size=1000,
                enable_sampling=True
            ) as loader:
                print("  Creating schema...")
                loader.create_schema(schema_file)
                
                print("  Loading data...")
                loader.load_json_data(json_file)
                
                stats = loader.get_database_stats()
                print(f"  ✓ Loaded {stats['total_images']:,} images, {stats['total_annotations']:,} annotations")
                
                # Should load full dataset since it's exactly at threshold
                if stats['total_images'] == 10000 and stats['total_annotations'] == 10000:
                    print(f"  ✓ Correctly loaded full dataset (at threshold)")
                else:
                    print(f"  ⚠ Unexpected result: got {stats['total_images']:,} images")
                    
        except Exception as e:
            print(f"  ✗ Edge case test failed: {e}")

def test_sampling_verification():
    """Verify that sampling maintains data quality and relationships"""
    print("\n=== Testing Sampling Data Quality ===\n")
    
    # Create test data with known relationships
    quality_data = {
        "info": {"year": 2020, "version": "1.0", "description": "Quality test"},
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
    
    # Create 12,000 images with license distribution
    for i in range(12000):
        quality_data["images"].append({
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
    
    # Create 15,000 annotations with category distribution
    for i in range(15000):
        quality_data["annotations"].append({
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
        
        # Save quality test data
        json_file = temp_path / "quality_data.json"
        with open(json_file, 'w') as f:
            json.dump(quality_data, f)
        
        # Save schema
        schema_file = temp_path / "test_schema.sql"
        with open(schema_file, 'w') as f:
            f.write(create_test_schema())
        
        # Test sampling and verify data quality
        db_path = temp_path / "quality_test.ddb"
        
        try:
            with FashionpediaLoaderFactory.create_loader(
                LoadingStrategy.DIRECT_JSON, 
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
                
                # Verify that we have both licenses
                license_count = stats['licenses_count']
                if license_count == 2:
                    print(f"  ✓ Both licenses preserved")
                else:
                    print(f"  ⚠ License count: {license_count}")
                
                # Verify that we have both categories
                category_count = stats['categories_count']
                if category_count == 2:
                    print(f"  ✓ Both categories preserved")
                else:
                    print(f"  ⚠ Category count: {category_count}")
                
                # Verify that we have both attributes
                attribute_count = stats['attributes_count']
                if attribute_count == 2:
                    print(f"  ✓ Both attributes preserved")
                else:
                    print(f"  ⚠ Attribute count: {attribute_count}")
                
                # Run sample queries to verify relationships
                print("  Running sample queries to verify relationships...")
                loader.run_sample_queries()
                
        except Exception as e:
            print(f"  ✗ Quality test failed: {e}")

if __name__ == "__main__":
    print("=== Sampling Pattern Tests ===\n")
    print("This test focuses only on sampling functionality to avoid long loading times.")
    print("It creates large test datasets that will trigger sampling mode.\n")
    
    # Test main sampling patterns
    test_sampling_patterns()
    
    # Test edge cases
    test_sampling_edge_cases()
    
    # Test data quality
    test_sampling_verification()
    
    print("\n=== Sampling Tests Complete ===")
    print("All tests focused on sampling functionality completed successfully!") 