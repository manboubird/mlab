#!/usr/bin/env python3
"""
Database comparison script for Fashionpedia datasets.
Compares statistics between the full dataset and the 1000-sample dataset.
"""

import sqlite3
import sys
from pathlib import Path

def get_table_stats(db_path, table_name):
    """Get basic statistics for a table."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Get sample data (first 3 rows)
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_data = cursor.fetchall()
        
        conn.close()
        
        return {
            'count': count,
            'columns': columns,
            'sample_data': sample_data
        }
    except Exception as e:
        return {
            'count': 0,
            'columns': [],
            'sample_data': [],
            'error': str(e)
        }

def print_table_comparison(db1_path, db2_path, table_name):
    """Print comparison for a specific table."""
    print(f"\n{'='*80}")
    print(f"TABLE: {table_name}")
    print(f"{'='*80}")
    
    stats1 = get_table_stats(db1_path, table_name)
    stats2 = get_table_stats(db2_path, table_name)
    
    print(f"{'Metric':<20} {'Full Dataset':<20} {'1000 Sample':<20} {'Ratio':<10}")
    print(f"{'-'*80}")
    
    if 'error' in stats1:
        print(f"Full dataset error: {stats1['error']}")
        return
    
    if 'error' in stats2:
        print(f"1000 sample error: {stats2['error']}")
        return
    
    # Row count comparison
    count1 = stats1['count']
    count2 = stats2['count']
    ratio = count2 / count1 if count1 > 0 else 0
    
    print(f"{'Row Count':<20} {count1:<20} {count2:<20} {ratio:.3f}")
    
    # Column count comparison
    col_count1 = len(stats1['columns'])
    col_count2 = len(stats2['columns'])
    
    print(f"{'Column Count':<20} {col_count1:<20} {col_count2:<20} {'N/A':<10}")
    
    # Show columns
    print(f"\nColumns in {table_name}:")
    print(f"Full dataset: {', '.join(stats1['columns'])}")
    print(f"1000 sample:  {', '.join(stats2['columns'])}")
    
    # Show sample data
    if stats1['sample_data'] and stats2['sample_data']:
        print(f"\nSample data (first 3 rows):")
        print(f"Full dataset:")
        for i, row in enumerate(stats1['sample_data']):
            print(f"  Row {i+1}: {row}")
        print(f"1000 sample:")
        for i, row in enumerate(stats2['sample_data']):
            print(f"  Row {i+1}: {row}")

def main():
    # Database paths
    db1_path = "fashionpedia.db"  # Full dataset
    db2_path = "fashionpeida-1000sample.db"  # 1000 sample dataset
    
    # Check if databases exist
    if not Path(db1_path).exists():
        print(f"Error: {db1_path} not found!")
        sys.exit(1)
    
    if not Path(db2_path).exists():
        print(f"Error: {db2_path} not found!")
        sys.exit(1)
    
    print("FASHIONPEDIA DATABASE COMPARISON")
    print("="*80)
    print(f"Full dataset: {db1_path}")
    print(f"1000 sample:  {db2_path}")
    
    # Tables to compare
    tables = [
        'info',
        'licenses', 
        'categories',
        'attributes',
        'images',
        'annotations',
        'annotation_attributes',
        'segmentations'
    ]
    
    # Compare each table
    for table in tables:
        print_table_comparison(db1_path, db2_path, table)
    
    # Additional analysis
    print(f"\n{'='*80}")
    print("ADDITIONAL ANALYSIS")
    print(f"{'='*80}")
    
    # Check relationships
    try:
        conn1 = sqlite3.connect(db1_path)
        conn2 = sqlite3.connect(db2_path)
        
        # Check image-annotation relationships
        cursor1 = conn1.cursor()
        cursor2 = conn2.cursor()
        
        cursor1.execute("""
            SELECT COUNT(DISTINCT i.id) as unique_images,
                   COUNT(a.id) as total_annotations,
                   COUNT(a.id) / COUNT(DISTINCT i.id) as annotations_per_image
            FROM images i
            LEFT JOIN annotations a ON i.id = a.image_id
        """)
        stats1 = cursor1.fetchone()
        
        cursor2.execute("""
            SELECT COUNT(DISTINCT i.id) as unique_images,
                   COUNT(a.id) as total_annotations,
                   COUNT(a.id) / COUNT(DISTINCT i.id) as annotations_per_image
            FROM images i
            LEFT JOIN annotations a ON i.id = a.image_id
        """)
        stats2 = cursor2.fetchone()
        
        print(f"\nRelationship Analysis:")
        print(f"{'Metric':<25} {'Full Dataset':<15} {'1000 Sample':<15} {'Ratio':<10}")
        print(f"{'-'*70}")
        print(f"{'Unique Images':<25} {stats1[0]:<15} {stats2[0]:<15} {stats2[0]/stats1[0]:.3f}")
        print(f"{'Total Annotations':<25} {stats1[1]:<15} {stats2[1]:<15} {stats2[1]/stats1[1]:.3f}")
        print(f"{'Annotations/Image':<25} {stats1[2]:<15.2f} {stats2[2]:<15.2f} {stats2[2]/stats1[2]:.3f}")
        
        # Check segmentation coverage
        cursor1.execute("""
            SELECT COUNT(DISTINCT a.id) as annotations_with_seg,
                   COUNT(s.id) as total_segmentations
            FROM annotations a
            LEFT JOIN segmentations s ON a.id = s.annotation_id
        """)
        seg_stats1 = cursor1.fetchone()
        
        cursor2.execute("""
            SELECT COUNT(DISTINCT a.id) as annotations_with_seg,
                   COUNT(s.id) as total_segmentations
            FROM annotations a
            LEFT JOIN segmentations s ON a.id = s.annotation_id
        """)
        seg_stats2 = cursor2.fetchone()
        
        print(f"\nSegmentation Coverage:")
        print(f"{'Metric':<25} {'Full Dataset':<15} {'1000 Sample':<15} {'Ratio':<10}")
        print(f"{'-'*70}")
        print(f"{'Annotations w/ Seg':<25} {seg_stats1[0]:<15} {seg_stats2[0]:<15} {seg_stats2[0]/seg_stats1[0]:.3f}")
        print(f"{'Total Segmentations':<25} {seg_stats1[1]:<15} {seg_stats2[1]:<15} {seg_stats2[1]/seg_stats1[1]:.3f}")
        
        conn1.close()
        conn2.close()
        
    except Exception as e:
        print(f"Error in additional analysis: {e}")

if __name__ == "__main__":
    main() 