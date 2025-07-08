#!/usr/bin/env python3
"""
Create test databases with sample data for demonstration.
"""

import sqlite3
import os

def create_test_database(filename, sample_size=100):
    """Create a test database with sample data."""
    
    # Remove existing file
    if os.path.exists(filename):
        os.remove(filename)
    
    conn = sqlite3.connect(filename)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE info (
            year INTEGER,
            version TEXT,
            description TEXT,
            contributor TEXT,
            url TEXT,
            date_created TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE licenses (
            id INTEGER PRIMARY KEY,
            name TEXT,
            url TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT,
            supercategory TEXT,
            level INTEGER,
            taxonomy_id INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE attributes (
            id INTEGER PRIMARY KEY,
            name TEXT,
            type TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            file_name TEXT,
            height INTEGER,
            width INTEGER,
            license_id INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE annotations (
            id INTEGER PRIMARY KEY,
            image_id INTEGER,
            category_id INTEGER,
            area REAL,
            bbox TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE annotation_attributes (
            id INTEGER PRIMARY KEY,
            annotation_id INTEGER,
            attribute_id INTEGER,
            value TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE segmentations (
            id INTEGER PRIMARY KEY,
            annotation_id INTEGER,
            segmentation TEXT
        )
    """)
    
    # Insert sample data
    cursor.execute("""
        INSERT INTO info VALUES (2020, '1.0', 'Test Fashionpedia dataset', 'Test Contributor', 'https://test.com', '2020-01-01')
    """)
    
    # Insert licenses
    for i in range(1, 6):
        cursor.execute(f"INSERT INTO licenses VALUES ({i}, 'License {i}', 'https://license{i}.com')")
    
    # Insert categories
    categories = [
        (1, 'Shirt', 'Clothing', 1, 1),
        (2, 'Pants', 'Clothing', 1, 2),
        (3, 'Dress', 'Clothing', 1, 3),
        (4, 'Shoes', 'Footwear', 1, 4),
        (5, 'Hat', 'Accessories', 1, 5)
    ]
    cursor.executemany("INSERT INTO categories VALUES (?, ?, ?, ?, ?)", categories)
    
    # Insert attributes
    attributes = [
        (1, 'Color', 'text'),
        (2, 'Size', 'text'),
        (3, 'Material', 'text'),
        (4, 'Pattern', 'text'),
        (5, 'Style', 'text')
    ]
    cursor.executemany("INSERT INTO attributes VALUES (?, ?, ?)", attributes)
    
    # Insert images
    for i in range(1, sample_size + 1):
        cursor.execute(f"INSERT INTO images VALUES ({i}, 'image_{i:06d}.jpg', 800, 600, {1 + (i % 4)})")
    
    # Insert annotations (2-3 per image)
    annotation_id = 1
    for image_id in range(1, sample_size + 1):
        num_annotations = 2 + (image_id % 2)  # 2 or 3 annotations per image
        for j in range(num_annotations):
            category_id = 1 + (annotation_id % 5)
            cursor.execute(f"""
                INSERT INTO annotations VALUES ({annotation_id}, {image_id}, {category_id}, 1000.0, '[100, 100, 200, 200]')
            """)
            annotation_id += 1
    
    # Insert annotation attributes
    attr_id = 1
    for annotation_id in range(1, annotation_id):
        num_attrs = 2 + (annotation_id % 3)  # 2-4 attributes per annotation
        for j in range(num_attrs):
            attribute_id = 1 + (j % 5)
            cursor.execute(f"""
                INSERT INTO annotation_attributes VALUES ({attr_id}, {annotation_id}, {attribute_id}, 'value_{attr_id}')
            """)
            attr_id += 1
    
    # Insert segmentations (for most annotations)
    seg_id = 1
    for annotation_id in range(1, annotation_id):
        if annotation_id % 3 != 0:  # Skip every 3rd annotation
            cursor.execute(f"""
                INSERT INTO segmentations VALUES ({seg_id}, {annotation_id}, '[[100, 100, 200, 100, 200, 200, 100, 200]]')
            """)
            seg_id += 1
    
    conn.commit()
    conn.close()
    
    print(f"Created test database: {filename}")
    print(f"Sample size: {sample_size} images")

def main():
    # Create two test databases for comparison
    create_test_database("fashionpedia_full.db", 1000)  # Full dataset simulation
    create_test_database("fashionpedia_sample.db", 100)  # Sample dataset
    
    print("\nTest databases created successfully!")
    print("You can now run SQL queries to compare them.")

if __name__ == "__main__":
    main() 