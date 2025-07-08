#!/usr/bin/env python3
"""
Ontology Generator for Fashionpedia Taxonomy
Converts Fashionpedia JSON categories to OWL/RDF ontology format
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL, XSD
from rdflib.term import Identifier

logger = logging.getLogger(__name__)

class FashionpediaOntologyGenerator:
    """
    Generates OWL ontology from Fashionpedia taxonomy data
    """
    
    def __init__(self, base_uri: str = "http://fashionpedia.org/ontology/"):
        """
        Initialize the ontology generator
        
        Args:
            base_uri: Base URI for the ontology
        """
        self.base_uri = base_uri
        self.graph = Graph()
        self.namespaces = {}
        self._setup_namespaces()
        
    def _setup_namespaces(self):
        """Setup RDF namespaces"""
        # Main ontology namespace
        self.fashionpedia = Namespace(self.base_uri)
        self.namespaces['fashionpedia'] = self.fashionpedia
        
        # Bind namespaces to graph
        self.graph.bind('fashionpedia', self.fashionpedia)
        self.graph.bind('rdf', RDF)
        self.graph.bind('rdfs', RDFS)
        self.graph.bind('owl', OWL)
        self.graph.bind('xsd', XSD)
        
    def _clean_name(self, name: str) -> str:
        """
        Clean category name for use as URI identifier
        
        Args:
            name: Raw category name
            
        Returns:
            Cleaned name suitable for URI
        """
        # Replace spaces and special characters
        cleaned = name.lower().replace(' ', '_').replace(',', '_').replace('-', '_')
        # Remove multiple underscores
        cleaned = '_'.join(filter(None, cleaned.split('_')))
        return cleaned
        
    def _create_uri(self, name: str, prefix: str = "category") -> URIRef:
        """
        Create URI for a category
        
        Args:
            name: Category name
            prefix: URI prefix
            
        Returns:
            URIRef for the category
        """
        clean_name = self._clean_name(name)
        return self.fashionpedia[f"{prefix}_{clean_name}"]
        
    def _add_class_hierarchy(self, supercategories: Set[str]):
        """
        Add supercategory classes to the ontology
        
        Args:
            supercategories: Set of supercategory names
        """
        # Add supercategory classes
        for supercat in supercategories:
            supercat_uri = self._create_uri(supercat, "supercategory")
            
            # Add class declaration
            self.graph.add((supercat_uri, RDF.type, OWL.Class))
            self.graph.add((supercat_uri, RDFS.label, Literal(supercat)))
            self.graph.add((supercat_uri, RDFS.comment, 
                           Literal(f"Supercategory for {supercat} clothing items")))
            
        # Add top-level ClothingItem class
        clothing_item_uri = self.fashionpedia.ClothingItem
        self.graph.add((clothing_item_uri, RDF.type, OWL.Class))
        self.graph.add((clothing_item_uri, RDFS.label, Literal("Clothing Item")))
        self.graph.add((clothing_item_uri, RDFS.comment, 
                       Literal("Base class for all clothing items")))
        
        # Make supercategories subclasses of ClothingItem
        for supercat in supercategories:
            supercat_uri = self._create_uri(supercat, "supercategory")
            self.graph.add((supercat_uri, RDFS.subClassOf, clothing_item_uri))
            
    def _add_category_classes(self, categories: List[Dict]):
        """
        Add individual category classes to the ontology
        
        Args:
            categories: List of category dictionaries
        """
        for category in categories:
            category_id = category['id']
            name = category['name']
            supercategory = category['supercategory']
            level = category['level']
            taxonomy_id = category['taxonomy_id']
            
            # Create URI for this category
            category_uri = self._create_uri(name, "category")
            supercat_uri = self._create_uri(supercategory, "supercategory")
            
            # Add class declaration
            self.graph.add((category_uri, RDF.type, OWL.Class))
            self.graph.add((category_uri, RDFS.label, Literal(name)))
            self.graph.add((category_uri, RDFS.comment, 
                           Literal(f"Clothing category: {name}")))
            
            # Add subclass relationship
            self.graph.add((category_uri, RDFS.subClassOf, supercat_uri))
            
            # Add data properties
            self.graph.add((category_uri, self.fashionpedia.hasCategoryId, 
                           Literal(category_id, datatype=XSD.integer)))
            self.graph.add((category_uri, self.fashionpedia.hasTaxonomyId, 
                           Literal(taxonomy_id)))
            self.graph.add((category_uri, self.fashionpedia.hasLevel, 
                           Literal(level, datatype=XSD.integer)))
            
    def _add_data_properties(self):
        """Add data properties to the ontology"""
        # Category ID property
        has_category_id = self.fashionpedia.hasCategoryId
        self.graph.add((has_category_id, RDF.type, OWL.DatatypeProperty))
        self.graph.add((has_category_id, RDFS.label, Literal("has category ID")))
        self.graph.add((has_category_id, RDFS.range, XSD.integer))
        
        # Taxonomy ID property
        has_taxonomy_id = self.fashionpedia.hasTaxonomyId
        self.graph.add((has_taxonomy_id, RDF.type, OWL.DatatypeProperty))
        self.graph.add((has_taxonomy_id, RDFS.label, Literal("has taxonomy ID")))
        self.graph.add((has_taxonomy_id, RDFS.range, XSD.string))
        
        # Level property
        has_level = self.fashionpedia.hasLevel
        self.graph.add((has_level, RDF.type, OWL.DatatypeProperty))
        self.graph.add((has_level, RDFS.label, Literal("has level")))
        self.graph.add((has_level, RDFS.range, XSD.integer))
        
    def _add_ontology_metadata(self):
        """Add ontology metadata"""
        ontology_uri = self.fashionpedia.FashionpediaOntology
        self.graph.add((ontology_uri, RDF.type, OWL.Ontology))
        self.graph.add((ontology_uri, RDFS.label, Literal("Fashionpedia Ontology")))
        self.graph.add((ontology_uri, RDFS.comment, 
                       Literal("Ontology for Fashionpedia clothing taxonomy")))
        self.graph.add((ontology_uri, OWL.versionInfo, Literal("1.0")))
        self.graph.add((ontology_uri, RDFS.seeAlso, 
                       Literal("https://github.com/cvdfoundation/fashionpedia")))
        
    def generate_ontology(self, categories_data: List[Dict]) -> Graph:
        """
        Generate OWL ontology from categories data
        
        Args:
            categories_data: List of category dictionaries from JSON
            
        Returns:
            RDFLib Graph containing the ontology
        """
        logger.info("Generating Fashionpedia ontology...")
        
        # Extract supercategories
        supercategories = set(cat['supercategory'] for cat in categories_data)
        logger.info(f"Found {len(supercategories)} supercategories: {supercategories}")
        
        # Add ontology metadata
        self._add_ontology_metadata()
        
        # Add data properties
        self._add_data_properties()
        
        # Add class hierarchy
        self._add_class_hierarchy(supercategories)
        
        # Add category classes
        self._add_category_classes(categories_data)
        
        logger.info(f"Generated ontology with {len(self.graph)} triples")
        return self.graph
        
    def save_ontology(self, graph: Graph, output_path: Path, format: str = "xml"):
        """
        Save ontology to file
        
        Args:
            graph: RDFLib Graph containing the ontology
            output_path: Output file path
            format: Output format (xml, turtle, n3, etc.)
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Serialize the graph
        graph.serialize(destination=str(output_path), format=format)
        logger.info(f"Ontology saved to {output_path} in {format} format")
        
    def load_categories_from_json(self, json_path: Path) -> List[Dict]:
        """
        Load categories from JSON file
        
        Args:
            json_path: Path to JSON file
            
        Returns:
            List of category dictionaries
        """
        logger.info(f"Loading categories from {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            if json_path.suffix == '.jsonl':
                # Handle JSONL format (one JSON object per line)
                categories = []
                for line in f:
                    if line.strip():
                        categories.append(json.loads(line))
            else:
                # Handle regular JSON format
                categories = json.load(f)
                
        logger.info(f"Loaded {len(categories)} categories")
        return categories
        
    def generate_from_file(self, input_path: Path, output_path: Path, 
                          format: str = "xml") -> Graph:
        """
        Generate ontology from JSON file and save to output
        
        Args:
            input_path: Path to input JSON file
            output_path: Path to output ontology file
            format: Output format
            
        Returns:
            RDFLib Graph containing the ontology
        """
        # Load categories
        categories = self.load_categories_from_json(input_path)
        
        # Generate ontology
        graph = self.generate_ontology(categories)
        
        # Save ontology
        self.save_ontology(graph, output_path, format)
        
        return graph 