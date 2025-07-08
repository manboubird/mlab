#!/usr/bin/env python3
"""
DuckDB Loader for Fashionpedia Data
Loads Fashionpedia JSON data into DuckDB database
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import duckdb
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from .base_loader import BaseFashionpediaLoader
import time
import random

logger = logging.getLogger(__name__)
console = Console()

class FashionpediaDuckDBLoader(BaseFashionpediaLoader):
    """
    Loads Fashionpedia JSON data into DuckDB database
    """
    
    def __init__(self, db_path: Path, sample_size: Optional[int] = None, enable_sampling: bool = False, small_dataset_mode: bool = False):
        """
        Initialize the DuckDB loader
        
        Args:
            db_path: Path to DuckDB database file
            sample_size: Number of samples to use when sampling is enabled
            enable_sampling: Whether to enable sampling mode
            small_dataset_mode: Whether to enable small dataset mode
        """
        super().__init__(db_path, sample_size, enable_sampling, small_dataset_mode)
        self.small_dataset_mode = small_dataset_mode
        self._warned_sampling = False
        
    def connect(self):
        """Connect to DuckDB database"""
        self.connection = duckdb.connect(str(self.db_path))
        logger.info(f"Connected to DuckDB database: {self.db_path}")
        
    def close(self):
        """Close DuckDB connection"""
        if self.connection:
            self.connection.close()
            logger.info("DuckDB connection closed")
            
    def create_schema(self, schema_file: Path):
        """
        Create database schema from SQL file
        
        Args:
            schema_file: Path to SQL schema file
        """
        # Use the schema without foreign key constraint on license
        schema_file_no_fk = schema_file.parent / "fashionpedia_schema_no_fk.sql"
        if schema_file_no_fk.exists():
            console.print(f"  Using schema without foreign key constraint: {schema_file_no_fk}")
            schema_file = schema_file_no_fk
            
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
            
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
            
        # Split and execute SQL statements
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Creating schema...", total=len(statements))
            
            for statement in statements:
                if statement:
                    self.connection.execute(statement)
                    progress.advance(task)
                    
        logger.info("Database schema created successfully")
        
    def _load_info(self, info_data: Dict[str, Any]):
        """Load info metadata"""
        if info_data:
            console.print(f"[blue]DEBUG: Loading info: {info_data}[/blue]")
            self.connection.execute("""
                INSERT INTO info (year, version, description, contributor, url, date_created)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                info_data.get('year'),
                info_data.get('version'),
                info_data.get('description'),
                info_data.get('contributor'),
                info_data.get('url'),
                info_data.get('date_created')
            ))
            
    def _load_licenses(self, licenses_data: List[Dict[str, Any]]):
        """Load licenses data"""
        if licenses_data:
            console.print(f"[blue]DEBUG: About to load {len(licenses_data)} licenses[/blue]")
            # Clear all tables to avoid foreign key constraint issues
            console.print("[blue]DEBUG: Clearing all tables to avoid foreign key constraints...[/blue]")
            self.connection.execute("DELETE FROM segmentations")
            self.connection.execute("DELETE FROM annotation_attributes")
            self.connection.execute("DELETE FROM annotations")
            self.connection.execute("DELETE FROM images")
            self.connection.execute("DELETE FROM attributes")
            self.connection.execute("DELETE FROM categories")
            self.connection.execute("DELETE FROM licenses")
            self.connection.execute("DELETE FROM info")
            
            loaded_license_ids = []
            json_license_ids = []
            # Insert new data, skip license id 0 or '0'
            for i, license_data in enumerate(licenses_data):
                license_id = license_data.get('id')
                json_license_ids.append(license_id)
                if i < 5:  # Print first 5 licenses for debugging
                    console.print(f"[cyan]DEBUG: Processing license {i}: id={license_id}, name={license_data.get('name')}[/cyan]")
                if license_id == 0 or license_id == '0':
                    console.print(f"[yellow]Skipping license with id 0[/yellow]")
                    continue
                try:
                    self.connection.execute("""
                        INSERT INTO licenses (id, name, url)
                        VALUES (?, ?, ?)
                    """, (
                        license_id,
                        license_data.get('name'),
                        license_data.get('url')
                    ))
                    loaded_license_ids.append(license_id)
                except Exception as e:
                    console.print(f"[red]ERROR inserting license {license_id}: {e}[/red]")
                    console.print(f"[red]License data: {license_data}[/red]")
                    raise
            console.print(f"[blue]DEBUG: License IDs from JSON: {json_license_ids}[/blue]")
            console.print(f"[blue]DEBUG: Loaded license IDs: {loaded_license_ids}[/blue]")
        console.print(f"[green]\u2713 Loaded {len(licenses_data)} licenses (excluding id 0)[/green]")
        
    def _load_categories(self, categories_data: List[Dict[str, Any]]):
        """Load categories data"""
        if categories_data:
            # Clear existing data
            self.connection.execute("DELETE FROM categories")
            
            # Insert new data
            for category_data in categories_data:
                self.connection.execute("""
                    INSERT INTO categories (id, name, supercategory, level, taxonomy_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    category_data.get('id'),
                    category_data.get('name'),
                    category_data.get('supercategory'),
                    category_data.get('level'),
                    category_data.get('taxonomy_id')
                ))
                
        console.print(f"[green]✓ Loaded {len(categories_data)} categories[/green]")
        
    def _load_attributes(self, attributes_data: List[Dict[str, Any]]):
        """Load attributes data"""
        if attributes_data:
            # Clear existing data
            self.connection.execute("DELETE FROM attributes")
            
            # Insert new data
            for attribute_data in attributes_data:
                self.connection.execute("""
                    INSERT INTO attributes (id, name, supercategory, level, taxonomy_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    attribute_data.get('id'),
                    attribute_data.get('name'),
                    attribute_data.get('supercategory'),
                    attribute_data.get('level'),
                    attribute_data.get('taxonomy_id')
                ))
                
        console.print(f"[green]✓ Loaded {len(attributes_data)} attributes[/green]")
        
    def _load_images(self, images_data: List[Dict[str, Any]]):
        """Load images data"""
        if images_data:
            # Clear existing data
            self.connection.execute("DELETE FROM images")
            
            license0_count = 0
            debug_image_prints = 0
            # Insert new data - no foreign key constraint, so we can insert any license value
            for image_data in images_data:
                license_id = image_data.get('license')
                if debug_image_prints < 10:
                    console.print(f"[cyan]DEBUG: Image id={image_data.get('id')} license={license_id}[/cyan]")
                    debug_image_prints += 1
                # Treat license 0 (int or str) as NULL for consistency
                if license_id == 0 or license_id == '0':
                    license0_count += 1
                    license_id = None
                
                self.connection.execute("""
                    INSERT INTO images (id, width, height, file_name, license, 
                                      time_captured, original_url, isstatic, kaggle_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    image_data.get('id'),
                    image_data.get('width'),
                    image_data.get('height'),
                    image_data.get('file_name'),
                    license_id,
                    image_data.get('time_captured'),
                    image_data.get('original_url'),
                    image_data.get('isstatic'),
                    image_data.get('kaggle_id')
                ))
            
            if license0_count > 0:
                console.print(f"[yellow]Note: {license0_count} images had license 0 and were set to NULL[/yellow]")
        console.print(f"[green]✓ Loaded {len(images_data)} images[/green]")
        
    def _load_annotations(self, annotations_data: List[Dict[str, Any]], segmentations_sample_size: int = None):
        """Load annotations data, with optional segmentation sampling"""
        if annotations_data:
            self.connection.execute("DELETE FROM annotation_attributes")
            self.connection.execute("DELETE FROM segmentations")
            self.connection.execute("DELETE FROM annotations")
            
            # Get existing image and category IDs
            existing_images = set()
            existing_categories = set()
            existing_attributes = set()
            
            result = self.connection.execute("SELECT id FROM images").fetchall()
            existing_images = {row[0] for row in result}
            
            result = self.connection.execute("SELECT id FROM categories").fetchall()
            existing_categories = {row[0] for row in result}
            
            result = self.connection.execute("SELECT id FROM attributes").fetchall()
            existing_attributes = {row[0] for row in result}
            
            inserted_annotation_ids = set()
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Loading annotations...", total=len(annotations_data))
                
                for annotation_data in annotations_data:
                    image_id = annotation_data.get('image_id')
                    category_id = annotation_data.get('category_id')
                    annotation_id = annotation_data.get('id')
                    # Only insert if both image and category exist
                    if image_id in existing_images and category_id in existing_categories:
                        bbox = annotation_data.get('bbox', [0, 0, 0, 0])
                        bbox_x, bbox_y, bbox_width, bbox_height = bbox
                        self.connection.execute("""
                            INSERT INTO annotations (id, image_id, category_id, 
                                                   bbox_x, bbox_y, bbox_width, bbox_height,
                                                   area, iscrowd)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            annotation_id,
                            image_id,
                            category_id,
                            bbox_x, bbox_y, bbox_width, bbox_height,
                            annotation_data.get('area'),
                            annotation_data.get('iscrowd')
                        ))
                        inserted_annotation_ids.add(annotation_id)
                        # Insert attribute relationships
                        attribute_ids = annotation_data.get('attribute_ids', [])
                        for attr_id in attribute_ids:
                            if attr_id in existing_attributes:
                                self.connection.execute("""
                                    INSERT INTO annotation_attributes (annotation_id, attribute_id)
                                    VALUES (?, ?)
                                """, (annotation_id, attr_id))
                    progress.advance(task)
            # Insert segmentations only for inserted annotation ids
            inserted_annotations = [ann for ann in annotations_data if ann.get('id') in inserted_annotation_ids]
            if segmentations_sample_size is not None:
                inserted_annotations = inserted_annotations[:segmentations_sample_size]
            console.print(f"[dim]Loading segmentations for {len(inserted_annotations):,} inserted annotations...[/dim]")
            
            start_time = time.time()
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Loading segmentations...", total=len(inserted_annotations))
                for annotation_data in inserted_annotations:
                    annotation_id = annotation_data.get('id')
                    segmentation = annotation_data.get('segmentation', [])
                    try:
                        for polygon_index, polygon in enumerate(segmentation):
                            for point_index in range(0, len(polygon), 2):
                                if point_index + 1 < len(polygon):
                                    x_coord = polygon[point_index]
                                    y_coord = polygon[point_index + 1]
                                    # Ensure coordinates are numeric
                                    if not isinstance(x_coord, (int, float)) or not isinstance(y_coord, (int, float)):
                                        console.print(f"[yellow]Warning: Non-numeric coordinates in annotation {annotation_id}: x={x_coord} (type: {type(x_coord)}), y={y_coord} (type: {type(y_coord)})[/yellow]")
                                        continue
                                    self.connection.execute("""
                                        INSERT INTO segmentations (annotation_id, polygon_index, 
                                                                  point_index, x_coord, y_coord)
                                        VALUES (?, ?, ?, ?, ?)
                                    """, (
                                        annotation_id,
                                        polygon_index,
                                        point_index // 2,
                                        x_coord,
                                        y_coord
                                    ))
                    except Exception as e:
                        console.print(f"[red]ERROR processing segmentation for annotation {annotation_id}: {e}[/red]")
                        console.print(f"[red]Segmentation data: {segmentation[:10]}...[/red]")
                        raise
                    progress.advance(task)
            
            segmentations_time = time.time() - start_time
            console.print(f"[dim]Segmentations loaded in {segmentations_time:.2f} seconds[/dim]")
        console.print(f"[green]✓ Loaded {len(annotations_data)} annotations[/green]")
        
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        
        Returns:
            Dictionary with database statistics
        """
        stats = {}
        
        # Count records in each table
        tables = ['info', 'licenses', 'categories', 'attributes', 'images', 'annotations', 
                 'annotation_attributes', 'segmentations']
        
        for table in tables:
            result = self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            stats[f"{table}_count"] = result[0] if result else 0
            
        # Get some sample statistics
        stats['total_annotations'] = stats['annotations_count']
        stats['total_images'] = stats['images_count']
        stats['total_categories'] = stats['categories_count']
        stats['total_attributes'] = stats['attributes_count']
        
        return stats
        
    def run_sample_queries(self):
        """Run sample queries to demonstrate the data"""
        console.print("\n[bold blue]Sample Queries:[/bold blue]")
        
        # Top categories by annotation count
        console.print("\n[cyan]Top 10 Categories by Annotation Count:[/cyan]")
        result = self.connection.execute("""
            SELECT c.name, c.supercategory, COUNT(a.id) as count
            FROM categories c
            LEFT JOIN annotations a ON c.id = a.category_id
            GROUP BY c.id, c.name, c.supercategory
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()
        
        for row in result:
            console.print(f"  {row[0]} ({row[1]}): {row[2]} annotations")
            
        # Top attributes by usage
        console.print("\n[cyan]Top 10 Attributes by Usage:[/cyan]")
        result = self.connection.execute("""
            SELECT attr.name, attr.supercategory, COUNT(aa.annotation_id) as count
            FROM attributes attr
            LEFT JOIN annotation_attributes aa ON attr.id = aa.attribute_id
            GROUP BY attr.id, attr.name, attr.supercategory
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()
        
        for row in result:
            console.print(f"  {row[0]} ({row[1]}): {row[2]} usages")
            
        # Image statistics
        console.print("\n[cyan]Image Statistics:[/cyan]")
        result = self.connection.execute("""
            SELECT 
                COUNT(*) as total_images,
                AVG(width) as avg_width,
                AVG(height) as avg_height,
                COUNT(DISTINCT license) as unique_licenses
            FROM images
        """).fetchone()
        
        console.print(f"  Total images: {result[0]}")
        console.print(f"  Average dimensions: {result[1]:.1f} x {result[2]:.1f}")
        console.print(f"  Unique licenses: {result[3]}")
        
    def export_to_csv(self, output_dir: Path):
        """
        Export database tables to CSV files
        
        Args:
            output_dir: Output directory for CSV files
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        tables = ['info', 'licenses', 'categories', 'attributes', 'images', 'annotations', 
                 'annotation_attributes', 'segmentations']
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Exporting to CSV...", total=len(tables))
            
            for table in tables:
                csv_path = output_dir / f"{table}.csv"
                self.connection.execute(f"COPY {table} TO '{csv_path}' (HEADER, DELIMITER ',')")
                progress.advance(task)
                
        console.print(f"[green]✓ Exported {len(tables)} tables to {output_dir}[/green]")

    def _load_with_sampling(self, data: Dict[str, Any]):
        # Determine sample sizes
        if self.small_dataset_mode:
            images_sample_size = 100
            annotations_sample_size = 200
            segmentations_sample_size = 200
            if self.sample_size not in (None, 1000) and not self._warned_sampling:
                console.print("[yellow]Warning: Both small_dataset_mode and sample_size are set. small_dataset_mode takes precedence (images=100, annotations=200, segmentations=200).[/yellow]")
                self._warned_sampling = True
        else:
            images_sample_size = self.sample_size
            annotations_sample_size = self.sample_size
            segmentations_sample_size = self.sample_size
        # Load metadata (no sampling needed)
        console.print("[bold blue]Applying intelligent sampling strategy...[/bold blue]")
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
        images_data = data.get('images', [])
        annotations_data = data.get('annotations', [])
        console.print(f"[dim]Original dataset: {len(images_data):,} images, {len(annotations_data):,} annotations[/dim]")
        # Sample images
        console.print(f"[dim]Sampling images (target: {images_sample_size:,})...[/dim]")
        sampled_image_ids = self._sample_images(images_data, images_sample_size)
        console.print(f"[dim]Sampled {len(sampled_image_ids):,} unique images[/dim]")
        # Sample annotations that reference sampled images
        console.print(f"[dim]Sampling annotations that reference sampled images (target: {annotations_sample_size:,})...[/dim]")
        sampled_annotations = self._sample_annotations_by_images(annotations_data, sampled_image_ids, annotations_sample_size)
        console.print(f"[dim]Sampled {len(sampled_annotations):,} annotations[/dim]")
        # Load sampled images
        sampled_images = [img for img in images_data if img['id'] in sampled_image_ids]
        console.print(f"[dim]Loading {len(sampled_images):,} sampled images...[/dim]")
        start_time = time.time()
        self._load_images(sampled_images)
        images_time = time.time() - start_time
        console.print(f"[dim]Images loaded in {images_time:.2f} seconds[/dim]")
        # Load sampled annotations (and segmentations)
        console.print(f"[dim]Loading {len(sampled_annotations):,} sampled annotations...[/dim]")
        start_time = time.time()
        self._load_annotations(sampled_annotations, segmentations_sample_size)
        annotations_time = time.time() - start_time
        console.print(f"[dim]Annotations (and segmentations) loaded in {annotations_time:.2f} seconds[/dim]")
        total_time = images_time + annotations_time
        console.print(f"[green]✓ Sampling complete: {len(sampled_images):,} images, {len(sampled_annotations):,} annotations, {segmentations_sample_size:,} segmentations (max) loaded in {total_time:.2f} seconds[/green]")

    def _sample_images(self, images_data: List[Dict[str, Any]], sample_size: int) -> set:
        if len(images_data) <= sample_size:
            return {img['id'] for img in images_data}
        license_groups = {}
        for img in images_data:
            license_id = img.get('license')
            if license_id not in license_groups:
                license_groups[license_id] = []
            license_groups[license_id].append(img)
        sampled_image_ids = set()
        remaining_samples = sample_size
        for license_id, images in license_groups.items():
            if remaining_samples <= 0:
                break
            group_size = len(images)
            group_sample_size = min(max(1, int(group_size * sample_size / len(images_data))), remaining_samples)
            sampled_from_group = random.sample(images, group_sample_size)
            sampled_image_ids.update(img['id'] for img in sampled_from_group)
            remaining_samples -= group_sample_size
        if remaining_samples > 0:
            remaining_images = [img for img in images_data if img['id'] not in sampled_image_ids]
            if remaining_images:
                additional_samples = random.sample(remaining_images, min(remaining_samples, len(remaining_images)))
                sampled_image_ids.update(img['id'] for img in additional_samples)
        return sampled_image_ids

    def _sample_annotations_by_images(self, annotations_data: List[Dict[str, Any]], sampled_image_ids: set, sample_size: int) -> list:
        relevant_annotations = [ann for ann in annotations_data if ann.get('image_id') in sampled_image_ids]
        if len(relevant_annotations) <= sample_size:
            return relevant_annotations
        category_groups = {}
        for ann in relevant_annotations:
            category_id = ann.get('category_id')
            if category_id not in category_groups:
                category_groups[category_id] = []
            category_groups[category_id].append(ann)
        sampled_annotations = []
        remaining_samples = sample_size
        for category_id, annotations in category_groups.items():
            if remaining_samples <= 0:
                break
            group_size = len(annotations)
            group_sample_size = min(max(1, int(group_size * sample_size / len(relevant_annotations))), remaining_samples)
            sampled_from_group = random.sample(annotations, group_sample_size)
            sampled_annotations.extend(sampled_from_group)
            remaining_samples -= group_sample_size
        if remaining_samples > 0:
            remaining_annotations = [ann for ann in relevant_annotations if ann not in sampled_annotations]
            if remaining_annotations:
                additional_samples = random.sample(remaining_annotations, min(remaining_samples, len(remaining_annotations)))
                sampled_annotations.extend(additional_samples)
        return sampled_annotations 