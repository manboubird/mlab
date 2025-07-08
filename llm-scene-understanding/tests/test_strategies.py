#!/usr/bin/env python3
"""
Test script to demonstrate and compare both loading strategies
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scene_understanding.loader_factory import FashionpediaLoaderFactory, LoadingStrategy

def test_loading_strategies():
    """Test and compare both loading strategies"""
    print("Testing Fashionpedia Loading Strategies")
    print("=" * 50)
    
    # Test paths
    schema_path = Path("data/01_raw/fashionpedia_schema.sql")
    json_path = Path("data/01_raw/instances_attributes_train2020.json")
    
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        return
        
    if not json_path.exists():
        print(f"Error: JSON file not found at {json_path}")
        return
    
    # Test with a small subset for comparison
    import json
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    # Create a small test dataset
    licenses = data.get('licenses', [])[:2]
    license_ids = {l['id'] for l in licenses}
    images = [img for img in data.get('images', [])[:20] if img.get('license') in license_ids]
    image_ids = {img['id'] for img in images}
    categories = data.get('categories', [])[:10]
    category_ids = {c['id'] for c in categories}
    attributes = data.get('attributes', [])[:20]
    attribute_ids = {a['id'] for a in attributes}
    annotations = [a for a in data.get('annotations', [])[:200] if a.get('image_id') in image_ids and a.get('category_id') in category_ids]
    
    test_data = {
        'info': data.get('info', {}),
        'licenses': licenses,
        'categories': categories,
        'attributes': attributes,
        'images': images,
        'annotations': annotations
    }
    
    # Save test data to temporary file
    test_json_path = Path("test_data.json")
    with open(test_json_path, 'w') as f:
        json.dump(test_data, f)
    
    strategies = [LoadingStrategy.DIRECT_JSON, LoadingStrategy.PARQUET_CONVERSION]
    results = {}
    
    for strategy in strategies:
        print(f"\n{'='*20} Testing {strategy.value.upper()} {'='*20}")
        
        # Get strategy info
        description = FashionpediaLoaderFactory.get_strategy_description(strategy)
        benefits = FashionpediaLoaderFactory.get_strategy_benefits(strategy)
        
        print(f"Strategy: {description}")
        print("Benefits:")
        for benefit in benefits:
            print(f"  • {benefit}")
        
        # Test the strategy
        db_path = Path(f"test_{strategy.value}.ddb")
        
        try:
            start_time = time.time()
            
            # Initialize loader
            with FashionpediaLoaderFactory.create_loader(strategy, db_path) as loader:
                # Create schema
                print("Creating database schema...")
                loader.create_schema(schema_path)
                
                # Load data
                print("Loading data...")
                loader.load_json_data(test_json_path)
                
                # Get statistics
                stats = loader.get_database_stats()
                end_time = time.time()
                
                results[strategy.value] = {
                    'time': end_time - start_time,
                    'stats': stats,
                    'db_size': db_path.stat().st_size if db_path.exists() else 0
                }
                
                print(f"✓ Completed in {end_time - start_time:.2f} seconds")
                print(f"✓ Database size: {db_path.stat().st_size / 1024:.1f} KB")
                print(f"✓ Total annotations: {stats['total_annotations']}")
                print(f"✓ Total images: {stats['total_images']}")
                
                # Cleanup temp files for Parquet strategy
                if strategy == LoadingStrategy.PARQUET_CONVERSION and hasattr(loader, 'cleanup_temp_files'):
                    loader.cleanup_temp_files()
                    
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    # Compare results
    print(f"\n{'='*20} COMPARISON RESULTS {'='*20}")
    
    if 'direct_json' in results and 'parquet_conversion' in results:
        direct = results['direct_json']
        parquet = results['parquet_conversion']
        
        print(f"Direct JSON Loading:")
        print(f"  Time: {direct['time']:.2f} seconds")
        print(f"  Database size: {direct['db_size'] / 1024:.1f} KB")
        
        print(f"\nParquet Conversion:")
        print(f"  Time: {parquet['time']:.2f} seconds")
        print(f"  Database size: {parquet['db_size'] / 1024:.1f} KB")
        
        print(f"\nComparison:")
        time_diff = ((parquet['time'] - direct['time']) / direct['time']) * 100
        size_diff = ((parquet['db_size'] - direct['db_size']) / direct['db_size']) * 100
        
        print(f"  Time difference: {time_diff:+.1f}%")
        print(f"  Size difference: {size_diff:+.1f}%")
        
        if parquet['time'] < direct['time']:
            print(f"  ✓ Parquet strategy is {abs(time_diff):.1f}% faster")
        else:
            print(f"  ⚠ Parquet strategy is {abs(time_diff):.1f}% slower")
            
        if parquet['db_size'] < direct['db_size']:
            print(f"  ✓ Parquet strategy uses {abs(size_diff):.1f}% less space")
        else:
            print(f"  ⚠ Parquet strategy uses {abs(size_diff):.1f}% more space")
    
    # Cleanup
    if test_json_path.exists():
        test_json_path.unlink()
    
    for strategy in strategies:
        db_path = Path(f"test_{strategy.value}.ddb")
        if db_path.exists():
            db_path.unlink()
    
    print(f"\nTest completed!")

if __name__ == "__main__":
    test_loading_strategies() 