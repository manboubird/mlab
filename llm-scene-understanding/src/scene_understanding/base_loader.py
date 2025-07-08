#!/usr/bin/env python3
"""
Base Loader for Fashionpedia Data
Provides common functionality for database loaders with sampling support
"""

import logging
import random
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from abc import ABC, abstractmethod
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.logging import RichHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True)])
logger = logging.getLogger("rich")

console = Console()

# Use orjson for faster JSON parsing
try:
    import orjson as json
    JSON_LIBRARY = "orjson"
except ImportError:
    import json
    JSON_LIBRARY = "standard"

class BaseFashionpediaLoader(ABC):
    """
    Base class for Fashionpedia data loaders with sampling support
    """
    
    def __init__(self, db_path: Path, sample_size: Optional[int] = None, enable_sampling: bool = False, small_dataset_mode: bool = False):
        """
        Initialize the loader
        
        Args:
            db_path: Path to the database file
            sample_size: Number of samples to use (overridden by small_dataset_mode if True)
            enable_sampling: Whether to enable sampling
            small_dataset_mode: Use reduced sampling numbers for faster processing
        """
        self.db_path = db_path
        self.enable_sampling = enable_sampling
        self.small_dataset_mode = small_dataset_mode
        
        # Set sample sizes based on mode
        if small_dataset_mode:
            self.sample_size = 200  # Default for annotations/segmentations
            self.image_sample_size = 100  # Smaller sample for images
            console.print(f"[yellow]Small dataset mode enabled: {self.image_sample_size} images, {self.sample_size} annotations[/yellow]")
        else:
            self.sample_size = sample_size or 1000
            self.image_sample_size = self.sample_size  # Same as annotations for normal mode
        self.connection = None
        
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        
    @abstractmethod
    def connect(self):
        """Connect to database"""
        pass
        
    @abstractmethod
    def close(self):
        """Close database connection"""
        pass
        
    @abstractmethod
    def create_schema(self, schema_file: Path):
        """Create database schema"""
        pass
        
    @abstractmethod
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        pass
        
    @abstractmethod
    def run_sample_queries(self):
        """Run sample queries"""
        pass
        
    def load_json_data(self, json_file: Path):
        """
        Load Fashionpedia JSON data with optional sampling
        
        Args:
            json_file: Path to Fashionpedia JSON file
        """
        if not json_file.exists():
            raise FileNotFoundError(f"JSON file not found: {json_file}")
            
        console.print(f"[bold blue]Loading Fashionpedia data from {json_file.name}...[/bold blue]")
        console.print(f"[dim]Using {JSON_LIBRARY} JSON library for parsing[/dim]")
        console.print(f"[dim]Sampling enabled: {self.enable_sampling}, Sample size: {self.sample_size:,}[/dim]")
        
        # Load JSON data with optimized parsing
        with open(json_file, 'rb') as f:  # Use binary mode for orjson
            data = json.loads(f.read())
            
        # Check if sampling should be enabled
        annotations_count = len(data.get('annotations', []))
        images_count = len(data.get('images', []))
        
        console.print(f"[dim]Dataset size: {images_count:,} images, {annotations_count:,} annotations[/dim]")
        
        if self.enable_sampling and (annotations_count > 10000 or images_count > 10000):
            console.print(f"[yellow]Large dataset detected: {annotations_count:,} annotations, {images_count:,} images[/yellow]")
            console.print(f"[yellow]Enabling sampling mode with {self.sample_size:,} samples[/yellow]")
            self._load_with_sampling(data)
        else:
            if self.enable_sampling:
                console.print(f"[green]Dataset size within limits, loading full dataset[/green]")
            else:
                console.print(f"[green]Sampling disabled, loading full dataset[/green]")
            self._load_full_data(data)
            
        logger.info("Fashionpedia data loaded successfully")
        self.commit()
        
    def _load_full_data(self, data: Dict[str, Any]):
        """Load full dataset without sampling"""
        self._load_info(data.get('info', {}))
        self._load_licenses(data.get('licenses', []))
        self._load_categories(data.get('categories', []))
        self._load_attributes(data.get('attributes', []))
        self._load_images(data.get('images', []))
        self._load_annotations(data.get('annotations', []))
        
    def _load_with_sampling(self, data: Dict[str, Any]):
        """Load dataset with sampling while maintaining referential integrity"""
        console.print("[bold blue]Applying intelligent sampling strategy...[/bold blue]")
        
        # Load metadata (no sampling needed)
        console.print("[dim]Loading metadata (no sampling applied)...[/dim]")
        try:
            console.print("[dim]Loading info...[/dim]")
            self._load_info(data.get('info', {}))
            console.print("[dim]Loading licenses...[/dim]")
            self._load_licenses(data.get('licenses', []))
            console.print("[dim]Loading categories...[/dim]")
            self._load_categories(data.get('categories', []))
            console.print("[dim]Loading attributes...[/dim]")
            self._load_attributes(data.get('attributes', []))
        except Exception as e:
            console.print(f"[red]Error during metadata loading: {e}[/red]")
            raise
        
        # Sample images and maintain relationships
        images_data = data.get('images', [])
        annotations_data = data.get('annotations', [])
        
        console.print(f"[dim]Original dataset: {len(images_data):,} images, {len(annotations_data):,} annotations[/dim]")
        
        if len(images_data) > 10000:
            # Sample images
            console.print(f"[dim]Sampling images (target: {self.image_sample_size:,})...[/dim]")
            sampled_image_ids = self._sample_images(images_data)
            console.print(f"[dim]Sampled {len(sampled_image_ids):,} unique images[/dim]")
            
            # Sample annotations that reference sampled images
            console.print(f"[dim]Sampling annotations that reference sampled images (target: {self.sample_size:,})...[/dim]")
            sampled_annotations = self._sample_annotations_by_images(annotations_data, sampled_image_ids)
            console.print(f"[dim]Sampled {len(sampled_annotations):,} annotations[/dim]")
        else:
            # If images are not too many, sample annotations directly
            console.print(f"[dim]Images within limit, sampling annotations directly (target: {self.sample_size:,})...[/dim]")
            sampled_image_ids = {img['id'] for img in images_data}
            sampled_annotations = self._sample_annotations(annotations_data)
            console.print(f"[dim]Sampled {len(sampled_annotations):,} annotations[/dim]")
            
        # Load sampled data with time measurement
        sampled_images = [img for img in images_data if img['id'] in sampled_image_ids]
        console.print(f"[dim]Loading {len(sampled_images):,} sampled images...[/dim]")
        start_time = time.time()
        self._load_images(sampled_images)
        images_time = time.time() - start_time
        console.print(f"[dim]Images loaded in {images_time:.2f} seconds[/dim]")
        
        console.print(f"[dim]Loading {len(sampled_annotations):,} sampled annotations...[/dim]")
        start_time = time.time()
        self._load_annotations(sampled_annotations)
        annotations_time = time.time() - start_time
        console.print(f"[dim]Annotations loaded in {annotations_time:.2f} seconds[/dim]")
        
        total_time = images_time + annotations_time
        console.print(f"[green]✓ Sampling complete: {len(sampled_images):,} images and {len(sampled_annotations):,} annotations loaded in {total_time:.2f} seconds[/green]")
        
    def _sample_images(self, images_data: List[Dict[str, Any]]) -> Set[int]:
        """Sample images while maintaining diversity"""
        if len(images_data) <= self.image_sample_size:
            return {img['id'] for img in images_data}
            
        # Use stratified sampling to maintain license diversity
        license_groups = {}
        for img in images_data:
            license_id = img.get('license')
            if license_id not in license_groups:
                license_groups[license_id] = []
            license_groups[license_id].append(img)
            
        sampled_image_ids = set()
        remaining_samples = self.image_sample_size
        
        # Sample from each license group proportionally
        for license_id, images in license_groups.items():
            if remaining_samples <= 0:
                break
                
            group_size = len(images)
            group_sample_size = min(
                max(1, int(group_size * self.image_sample_size / len(images_data))),
                remaining_samples
            )
            
            sampled_from_group = random.sample(images, group_sample_size)
            sampled_image_ids.update(img['id'] for img in sampled_from_group)
            remaining_samples -= group_sample_size
            
        # If we still have samples to allocate, distribute randomly
        if remaining_samples > 0:
            remaining_images = [img for img in images_data if img['id'] not in sampled_image_ids]
            if remaining_images:
                additional_samples = random.sample(remaining_images, min(remaining_samples, len(remaining_images)))
                sampled_image_ids.update(img['id'] for img in additional_samples)
                
        return sampled_image_ids
        
    def _sample_annotations_by_images(self, annotations_data: List[Dict[str, Any]], 
                                    sampled_image_ids: Set[int]) -> List[Dict[str, Any]]:
        """Sample annotations that reference sampled images"""
        # Get annotations that reference sampled images
        relevant_annotations = [
            ann for ann in annotations_data 
            if ann.get('image_id') in sampled_image_ids
        ]
        
        if len(relevant_annotations) <= self.sample_size:
            return relevant_annotations
            
        # Sample annotations while maintaining category diversity
        category_groups = {}
        for ann in relevant_annotations:
            category_id = ann.get('category_id')
            if category_id not in category_groups:
                category_groups[category_id] = []
            category_groups[category_id].append(ann)
            
        sampled_annotations = []
        remaining_samples = self.sample_size
        
        # Sample from each category group proportionally
        for category_id, annotations in category_groups.items():
            if remaining_samples <= 0:
                break
                
            group_size = len(annotations)
            group_sample_size = min(
                max(1, int(group_size * self.sample_size / len(relevant_annotations))),
                remaining_samples
            )
            
            sampled_from_group = random.sample(annotations, group_sample_size)
            sampled_annotations.extend(sampled_from_group)
            remaining_samples -= group_sample_size
            
        # If we still have samples to allocate, distribute randomly
        if remaining_samples > 0:
            remaining_annotations = [
                ann for ann in relevant_annotations 
                if ann not in sampled_annotations
            ]
            if remaining_annotations:
                additional_samples = random.sample(remaining_annotations, min(remaining_samples, len(remaining_annotations)))
                sampled_annotations.extend(additional_samples)
                
        return sampled_annotations
        
    def _sample_annotations(self, annotations_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sample annotations directly when images are not too many"""
        if len(annotations_data) <= self.sample_size:
            return annotations_data
            
        # Use stratified sampling to maintain category diversity
        category_groups = {}
        for ann in annotations_data:
            category_id = ann.get('category_id')
            if category_id not in category_groups:
                category_groups[category_id] = []
            category_groups[category_id].append(ann)
            
        sampled_annotations = []
        remaining_samples = self.sample_size
        
        # Sample from each category group proportionally
        for category_id, annotations in category_groups.items():
            if remaining_samples <= 0:
                break
                
            group_size = len(annotations)
            group_sample_size = min(
                max(1, int(group_size * self.sample_size / len(annotations_data))),
                remaining_samples
            )
            
            sampled_from_group = random.sample(annotations, group_sample_size)
            sampled_annotations.extend(sampled_from_group)
            remaining_samples -= group_sample_size
            
        # If we still have samples to allocate, distribute randomly
        if remaining_samples > 0:
            remaining_annotations = [
                ann for ann in annotations_data 
                if ann not in sampled_annotations
            ]
            if remaining_annotations:
                additional_samples = random.sample(remaining_annotations, min(remaining_samples, len(remaining_annotations)))
                sampled_annotations.extend(additional_samples)
                
        return sampled_annotations
        
    @abstractmethod
    def _load_info(self, info_data: Dict[str, Any]):
        """Load info metadata"""
        pass
        
    @abstractmethod
    def _load_licenses(self, licenses_data: List[Dict[str, Any]]):
        """Load licenses data"""
        pass
        
    @abstractmethod
    def _load_categories(self, categories_data: List[Dict[str, Any]]):
        """Load categories data"""
        pass
        
    @abstractmethod
    def _load_attributes(self, attributes_data: List[Dict[str, Any]]):
        """Load attributes data"""
        pass
        
    @abstractmethod
    def _load_images(self, images_data: List[Dict[str, Any]]):
        """Load images data"""
        pass
        
    @abstractmethod
    def _load_annotations(self, annotations_data: List[Dict[str, Any]]):
        """Load annotations data"""
        pass 