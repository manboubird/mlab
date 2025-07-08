#!/usr/bin/env python3
"""
Test script for Fashionpedia DuckDB loader
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scene_understanding.duckdb_loader import FashionpediaDuckDBLoader

def test_duckdb_loader():
    """Test the DuckDB loader functionality"""
    print("Testing Fashionpedia DuckDB loader...")
    
    # Test paths
    schema_path = Path("data/01_raw/fashionpedia_schema.sql")
    json_path = Path("data/01_raw/instances_attributes_train2020.json")
    db_path = Path("test_fashionpedia.ddb")
    
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        return
        
    if not json_path.exists():
        print(f"Error: JSON file not found at {json_path}")
        return
    
    try:
        # Initialize loader
        with FashionpediaDuckDBLoader(db_path) as loader:
            # Create schema
            print("Creating database schema...")
            loader.create_schema(schema_path)
            
            # Load data (use a small subset for testing)
            print("Loading sample data...")
            
            # For testing, we'll load just a small portion
            import json
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            # Create a small test dataset
            licenses = data.get('licenses', [])[:2]  # First 2 licenses
            license_ids = {l['id'] for l in licenses}
            images = [img for img in data.get('images', [])[:10] if img.get('license') in license_ids]
            image_ids = {img['id'] for img in images}
            categories = data.get('categories', [])[:5]
            category_ids = {c['id'] for c in categories}
            attributes = data.get('attributes', [])[:10]
            attribute_ids = {a['id'] for a in attributes}
            # Only annotations that reference loaded images and categories
            annotations = [a for a in data.get('annotations', [])[:100] if a.get('image_id') in image_ids and a.get('category_id') in category_ids]
            
            test_data = {
                'info': data.get('info', {}),
                'licenses': licenses,
                'categories': categories,
                'attributes': attributes,
                'images': images,
                'annotations': annotations
            }
            
            # Clear all tables in correct order
            loader.connection.execute("DELETE FROM annotation_attributes")
            loader.connection.execute("DELETE FROM segmentations")
            loader.connection.execute("DELETE FROM annotations")
            loader.connection.execute("DELETE FROM images")
            loader.connection.execute("DELETE FROM categories")
            loader.connection.execute("DELETE FROM attributes")
            loader.connection.execute("DELETE FROM licenses")
            loader.connection.execute("DELETE FROM info")
            
            # Load test data
            loader._load_info(test_data['info'])
            loader._load_licenses(test_data['licenses'])
            loader._load_categories(test_data['categories'])
            loader._load_attributes(test_data['attributes'])
            loader._load_images(test_data['images'])
            loader._load_annotations(test_data['annotations'])
            
            # Get statistics
            stats = loader.get_database_stats()
            print(f"\nDatabase Statistics:")
            print(f"  Total annotations: {stats['total_annotations']}")
            print(f"  Total images: {stats['total_images']}")
            print(f"  Total categories: {stats['total_categories']}")
            print(f"  Total attributes: {stats['total_attributes']}")
            
            # Run sample queries
            print(f"\nSample Queries:")
            loader.run_sample_queries()
            
            print(f"\nTest completed successfully!")
            print(f"Database saved to: {db_path}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_duckdb_loader() 