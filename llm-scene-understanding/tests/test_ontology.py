#!/usr/bin/env python3
"""
Test script for Fashionpedia ontology generation
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scene_understanding.ontology_generator import FashionpediaOntologyGenerator

def test_ontology_generation():
    """Test the ontology generation functionality"""
    print("Testing Fashionpedia ontology generation...")
    
    # Initialize generator
    generator = FashionpediaOntologyGenerator()
    
    # Test with sample data
    sample_categories = [
        {
            "id": 0,
            "name": "shirt, blouse",
            "supercategory": "upperbody",
            "level": 2,
            "taxonomy_id": "combo000000"
        },
        {
            "id": 1,
            "name": "top, t-shirt, sweatshirt",
            "supercategory": "upperbody",
            "level": 2,
            "taxonomy_id": "combo000001"
        },
        {
            "id": 6,
            "name": "pants",
            "supercategory": "lowerbody",
            "level": 2,
            "taxonomy_id": "obj000013_00"
        }
    ]
    
    # Generate ontology
    graph = generator.generate_ontology(sample_categories)
    
    print(f"Generated ontology with {len(graph)} triples")
    
    # Save to file
    output_path = Path("test_ontology.owl")
    generator.save_ontology(graph, output_path, format="xml")
    print(f"Saved ontology to {output_path}")
    
    # Print some triples
    print("\nSample triples:")
    for i, (s, p, o) in enumerate(graph):
        if i < 10:  # Show first 10 triples
            print(f"  {s} {p} {o}")
        else:
            break
    
    print("\nTest completed successfully!")

if __name__ == "__main__":
    test_ontology_generation() 