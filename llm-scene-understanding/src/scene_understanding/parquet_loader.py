#!/usr/bin/env python3
"""
Parquet-based Loader for Fashionpedia Data
Converts JSON to Parquet format and then loads into DuckDB for better performance
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import duckdb
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from .base_loader import BaseFashionpediaLoader
import random

logger = logging.getLogger(__name__)
console = Console()

class FashionpediaParquetLoader(BaseFashionpediaLoader):
    """
    Loads Fashionpedia JSON data into DuckDB via Parquet conversion
    Provides better performance and compression compared to direct JSON loading
    """
    
    def __init__(self, db_path: Path, temp_dir: Optional[Path] = None, 
                 sample_size: Optional[int] = None, enable_sampling: bool = False,
                 small_dataset_mode: bool = False, no_cleanup: bool = False):
        """
        Initialize the Parquet loader
        
        Args:
            db_path: Path to DuckDB database file
            temp_dir: Directory for temporary Parquet files (defaults to same dir as db)
            sample_size: Number of samples to use when sampling is enabled
            enable_sampling: Whether to enable sampling mode
            small_dataset_mode: Whether to enable small dataset mode
            no_cleanup: Whether to keep intermediate Parquet files (default: False)
        """
        super().__init__(db_path, sample_size, enable_sampling, small_dataset_mode)
        self.temp_dir = temp_dir or db_path.parent / "temp_parquet"
        self.small_dataset_mode = small_dataset_mode
        self.no_cleanup = no_cleanup
        
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
        
    def json_to_parquet(self, json_file: Path):
        """
        Convert Fashionpedia JSON to Parquet files
        
        Args:
            json_file: Path to Fashionpedia JSON file
        """
        if not json_file.exists():
            raise FileNotFoundError(f"JSON file not found: {json_file}")
            
        # Create temp directory
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        console.print(f"[bold blue]Converting JSON to Parquet format...[/bold blue]")
        
        # Load JSON data
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        # Check if sampling should be enabled
        annotations_count = len(data.get('annotations', []))
        images_count = len(data.get('images', []))
        
        if self.enable_sampling and (annotations_count > 10000 or images_count > 10000):
            console.print(f"[yellow]Large dataset detected: {annotations_count:,} annotations, {images_count:,} images[/yellow]")
            console.print(f"[yellow]Enabling sampling mode with {self.sample_size:,} samples[/yellow]")
            self._convert_with_sampling(data)
        else:
            if self.enable_sampling:
                console.print(f"[green]Dataset size within limits, converting full dataset[/green]")
            self._convert_full_data(data)
            
        logger.info("JSON to Parquet conversion completed")
        
    def _convert_full_data(self, data: Dict[str, Any]):
        """Convert full dataset without sampling"""
        self._convert_info_to_parquet(data.get('info', {}))
        self._convert_licenses_to_parquet(data.get('licenses', []))
        self._convert_categories_to_parquet(data.get('categories', []))
        self._convert_attributes_to_parquet(data.get('attributes', []))
        self._convert_images_to_parquet(data.get('images', []))
        self._convert_annotations_to_parquet(data.get('annotations', []))
        
    def _convert_with_sampling(self, data: Dict[str, Any]):
        """Convert dataset with sampling while maintaining referential integrity"""
        console.print("[bold blue]Applying intelligent sampling strategy...[/bold blue]")
        
        # Convert metadata (no sampling needed)
        console.print("[dim]Converting metadata (no sampling applied)...[/dim]")
        self._convert_info_to_parquet(data.get('info', {}))
        self._convert_licenses_to_parquet(data.get('licenses', []))
        self._convert_categories_to_parquet(data.get('categories', []))
        self._convert_attributes_to_parquet(data.get('attributes', []))
        
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
            
        # Convert sampled data
        sampled_images = [img for img in images_data if img['id'] in sampled_image_ids]
        console.print(f"[dim]Converting {len(sampled_images):,} sampled images to Parquet...[/dim]")
        self._convert_images_to_parquet(sampled_images)
        
        console.print(f"[dim]Converting {len(sampled_annotations):,} sampled annotations to Parquet...[/dim]")
        self._convert_annotations_to_parquet(sampled_annotations)
        
        console.print(f"[green]✓ Converted {len(sampled_images):,} images and {len(sampled_annotations):,} annotations to Parquet[/green]")
        
    def _convert_info_to_parquet(self, info_data: Dict[str, Any]):
        """Convert info metadata to Parquet"""
        if info_data:
            parquet_path = self.temp_dir / "info.parquet"
            # Create temp table with explicit column types
            self.connection.execute("""
                CREATE TEMP TABLE temp_info (
                    year INTEGER,
                    version VARCHAR,
                    description VARCHAR,
                    contributor VARCHAR,
                    url VARCHAR,
                    date_created VARCHAR
                )
            """)
            # Insert data
            self.connection.execute("""
                INSERT INTO temp_info VALUES (?, ?, ?, ?, ?, ?)
            """, (
                info_data.get('year'),
                info_data.get('version'),
                info_data.get('description'),
                info_data.get('contributor'),
                info_data.get('url'),
                info_data.get('date_created')
            ))
            # Export to Parquet
            self.connection.execute(f"COPY temp_info TO '{parquet_path}' (FORMAT parquet)")
            self.connection.execute("DROP TABLE temp_info")
            
    def _convert_licenses_to_parquet(self, licenses_data: List[Dict[str, Any]]):
        """Convert licenses data to Parquet"""
        if licenses_data:
            parquet_path = self.temp_dir / "licenses.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_licenses (
                    id INTEGER,
                    name VARCHAR,
                    url VARCHAR
                )
            """)
            for license_data in licenses_data:
                self.connection.execute("""
                    INSERT INTO temp_licenses VALUES (?, ?, ?)
                """, (
                    license_data.get('id'),
                    license_data.get('name'),
                    license_data.get('url')
                ))
            self.connection.execute(f"COPY temp_licenses TO '{parquet_path}' (FORMAT parquet)")
            self.connection.execute("DROP TABLE temp_licenses")
        console.print(f"[green]✓ Converted {len(licenses_data)} licenses to Parquet[/green]")
        
    def _convert_categories_to_parquet(self, categories_data: List[Dict[str, Any]]):
        """Convert categories data to Parquet"""
        if categories_data:
            parquet_path = self.temp_dir / "categories.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_categories (
                    id INTEGER,
                    name VARCHAR,
                    supercategory VARCHAR,
                    level INTEGER,
                    taxonomy_id VARCHAR
                )
            """)
            for category_data in categories_data:
                self.connection.execute("""
                    INSERT INTO temp_categories VALUES (?, ?, ?, ?, ?)
                """, (
                    category_data.get('id'),
                    category_data.get('name'),
                    category_data.get('supercategory'),
                    category_data.get('level'),
                    category_data.get('taxonomy_id')
                ))
            self.connection.execute(f"COPY temp_categories TO '{parquet_path}' (FORMAT parquet)")
            self.connection.execute("DROP TABLE temp_categories")
        console.print(f"[green]✓ Converted {len(categories_data)} categories to Parquet[/green]")
        
    def _convert_attributes_to_parquet(self, attributes_data: List[Dict[str, Any]]):
        """Convert attributes data to Parquet"""
        if attributes_data:
            parquet_path = self.temp_dir / "attributes.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_attributes (
                    id INTEGER,
                    name VARCHAR,
                    supercategory VARCHAR,
                    level INTEGER,
                    taxonomy_id VARCHAR
                )
            """)
            for attribute_data in attributes_data:
                self.connection.execute("""
                    INSERT INTO temp_attributes VALUES (?, ?, ?, ?, ?)
                """, (
                    attribute_data.get('id'),
                    attribute_data.get('name'),
                    attribute_data.get('supercategory'),
                    attribute_data.get('level'),
                    attribute_data.get('taxonomy_id')
                ))
            self.connection.execute(f"COPY temp_attributes TO '{parquet_path}' (FORMAT parquet)")
            self.connection.execute("DROP TABLE temp_attributes")
        console.print(f"[green]✓ Converted {len(attributes_data)} attributes to Parquet[/green]")
        
    def _convert_images_to_parquet(self, images_data: List[Dict[str, Any]]):
        """Convert images data to Parquet"""
        if images_data:
            parquet_path = self.temp_dir / "images.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_images (
                    id INTEGER,
                    width INTEGER,
                    height INTEGER,
                    file_name VARCHAR,
                    license INTEGER,
                    time_captured VARCHAR,
                    original_url VARCHAR,
                    isstatic BOOLEAN,
                    kaggle_id VARCHAR
                )
            """)
            for image_data in images_data:
                self.connection.execute("""
                    INSERT INTO temp_images VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    image_data.get('id'),
                    image_data.get('width'),
                    image_data.get('height'),
                    image_data.get('file_name'),
                    image_data.get('license'),
                    image_data.get('time_captured'),
                    image_data.get('original_url'),
                    image_data.get('isstatic'),
                    image_data.get('kaggle_id')
                ))
            self.connection.execute(f"COPY temp_images TO '{parquet_path}' (FORMAT parquet)")
            self.connection.execute("DROP TABLE temp_images")
        console.print(f"[green]✓ Converted {len(images_data)} images to Parquet[/green]")
        
    def _convert_annotations_to_parquet(self, annotations_data: List[Dict[str, Any]]):
        """Convert annotations data to Parquet"""
        if annotations_data:
            # Convert main annotations
            annotations_parquet_path = self.temp_dir / "annotations.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_annotations (
                    id INTEGER,
                    image_id INTEGER,
                    category_id INTEGER,
                    bbox_x DOUBLE,
                    bbox_y DOUBLE,
                    bbox_width DOUBLE,
                    bbox_height DOUBLE,
                    area DOUBLE,
                    iscrowd BOOLEAN
                )
            """)
            
            # Convert annotation attributes
            annotation_attributes_parquet_path = self.temp_dir / "annotation_attributes.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_annotation_attributes (
                    annotation_id INTEGER,
                    attribute_id INTEGER
                )
            """)
            
            # Convert segmentations
            segmentations_parquet_path = self.temp_dir / "segmentations.parquet"
            self.connection.execute("""
                CREATE TEMP TABLE temp_segmentations (
                    annotation_id INTEGER,
                    polygon_index INTEGER,
                    point_index INTEGER,
                    x_coord DOUBLE,
                    y_coord DOUBLE
                )
            """)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Converting annotations...", total=len(annotations_data))
                
                for annotation_data in annotations_data:
                    # Insert main annotation
                    bbox = annotation_data.get('bbox', [0, 0, 0, 0])
                    bbox_x, bbox_y, bbox_width, bbox_height = bbox
                    self.connection.execute("""
                        INSERT INTO temp_annotations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        annotation_data.get('id'),
                        annotation_data.get('image_id'),
                        annotation_data.get('category_id'),
                        bbox_x, bbox_y, bbox_width, bbox_height,
                        annotation_data.get('area'),
                        annotation_data.get('iscrowd')
                    ))
                    
                    # Insert attribute relationships
                    attribute_ids = annotation_data.get('attribute_ids', [])
                    for attr_id in attribute_ids:
                        self.connection.execute("""
                            INSERT INTO temp_annotation_attributes VALUES (?, ?)
                        """, (annotation_data.get('id'), attr_id))
                    
                    # Insert segmentations
                    segmentation = annotation_data.get('segmentation', [])
                    for polygon_index, polygon in enumerate(segmentation):
                        for point_index in range(0, len(polygon), 2):
                            if point_index + 1 < len(polygon):
                                x_coord = polygon[point_index]
                                y_coord = polygon[point_index + 1]
                                
                                # Check if coordinates are numeric
                                try:
                                    x_coord = float(x_coord)
                                    y_coord = float(y_coord)
                                except (ValueError, TypeError):
                                    # Skip invalid coordinates and log warning
                                    console.print(f"[yellow]Warning: Non-numeric coordinates in annotation {annotation_data.get('id')}: x={x_coord} (type: {type(x_coord)}), y={y_coord} (type: {type(y_coord)})[/yellow]")
                                    continue
                                
                                self.connection.execute("""
                                    INSERT INTO temp_segmentations VALUES (?, ?, ?, ?, ?)
                                """, (
                                    annotation_data.get('id'),
                                    polygon_index,
                                    point_index // 2,
                                    x_coord,
                                    y_coord
                                ))
                    
                    progress.advance(task)
            
            # Export to Parquet
            self.connection.execute(f"COPY temp_annotations TO '{annotations_parquet_path}' (FORMAT parquet)")
            self.connection.execute(f"COPY temp_annotation_attributes TO '{annotation_attributes_parquet_path}' (FORMAT parquet)")
            self.connection.execute(f"COPY temp_segmentations TO '{segmentations_parquet_path}' (FORMAT parquet)")
            
            # Clean up temp tables
            self.connection.execute("DROP TABLE temp_annotations")
            self.connection.execute("DROP TABLE temp_annotation_attributes")
            self.connection.execute("DROP TABLE temp_segmentations")
            
        console.print(f"[green]✓ Converted {len(annotations_data)} annotations to Parquet[/green]")
        
    def load_parquet_to_duckdb(self):
        """Load Parquet files into DuckDB database"""
        console.print("[bold blue]Loading Parquet files into DuckDB...[/bold blue]")
        
        # Load each Parquet file into corresponding table
        parquet_files = {
            'info': 'info.parquet',
            'licenses': 'licenses.parquet',
            'categories': 'categories.parquet',
            'attributes': 'attributes.parquet',
            'images': 'images.parquet',
            'annotations': 'annotations.parquet',
            'annotation_attributes': 'annotation_attributes.parquet',
            'segmentations': 'segmentations.parquet'
        }
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Loading Parquet files...", total=len(parquet_files))
            
            for table_name, parquet_file in parquet_files.items():
                parquet_path = self.temp_dir / parquet_file
                if parquet_path.exists():
                    # Instead of DELETE, use INSERT OR REPLACE to avoid foreign key issues
                    try:
                        self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM '{parquet_path}'")
                    except Exception as e:
                        # If INSERT fails, try to clear the table first
                        console.print(f"[yellow]Warning: Direct insert failed for {table_name}, clearing table first...[/yellow]")
                        self.connection.execute(f"DELETE FROM {table_name}")
                        self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM '{parquet_path}'")
                progress.advance(task)
                
        console.print("[green]✓ Parquet files loaded into DuckDB[/green]")
        
    def load_json_data(self, json_file: Path):
        """
        Load Fashionpedia JSON data via Parquet conversion
        
        Args:
            json_file: Path to Fashionpedia JSON file
        """
        # Convert JSON to Parquet
        self.json_to_parquet(json_file)
        
        # Load Parquet files into DuckDB
        self.load_parquet_to_duckdb()
        
        # Clean up temporary files unless no_cleanup is enabled
        if not self.no_cleanup:
            self.cleanup_temp_files()
        else:
            console.print(f"[yellow]Keeping intermediate Parquet files in: {self.temp_dir}[/yellow]")
            logger.info(f"Intermediate Parquet files preserved in: {self.temp_dir}")
        
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
        
    def cleanup_temp_files(self):
        """Clean up temporary Parquet files"""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            
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
        
    def _load_info(self, info_data: Dict[str, Any]):
        """Load info metadata - not used in Parquet loader"""
        pass
        
    def _load_licenses(self, licenses_data: List[Dict[str, Any]]):
        """Load licenses data - not used in Parquet loader"""
        pass
        
    def _load_categories(self, categories_data: List[Dict[str, Any]]):
        """Load categories data - not used in Parquet loader"""
        pass
        
    def _load_attributes(self, attributes_data: List[Dict[str, Any]]):
        """Load attributes data - not used in Parquet loader"""
        pass
        
    def _load_images(self, images_data: List[Dict[str, Any]]):
        """Load images data - not used in Parquet loader"""
        pass
        
    def _load_annotations(self, annotations_data: List[Dict[str, Any]]):
        """Load annotations data - not used in Parquet loader"""
        pass
        
    def _sample_images(self, images_data: List[Dict[str, Any]]) -> set:
        """
        Sample images randomly while maintaining diversity
        
        Args:
            images_data: List of image dictionaries
            
        Returns:
            Set of sampled image IDs
        """
        if not images_data:
            return set()
            
        # Use image_sample_size from base class
        sample_size = min(self.image_sample_size, len(images_data))
        
        # Random sampling
        sampled_images = random.sample(images_data, sample_size)
        sampled_ids = {img['id'] for img in sampled_images}
        
        return sampled_ids
        
    def _sample_annotations_by_images(self, annotations_data: List[Dict[str, Any]], 
                                    sampled_image_ids: set) -> List[Dict[str, Any]]:
        """
        Sample annotations that reference sampled images
        
        Args:
            annotations_data: List of annotation dictionaries
            sampled_image_ids: Set of sampled image IDs
            
        Returns:
            List of sampled annotation dictionaries
        """
        if not annotations_data:
            return []
            
        # Filter annotations that reference sampled images
        relevant_annotations = [ann for ann in annotations_data if ann.get('image_id') in sampled_image_ids]
        
        if not relevant_annotations:
            return []
            
        # Sample from relevant annotations
        sample_size = min(self.sample_size, len(relevant_annotations))
        sampled_annotations = random.sample(relevant_annotations, sample_size)
        
        return sampled_annotations
        
    def _sample_annotations(self, annotations_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sample annotations randomly
        
        Args:
            annotations_data: List of annotation dictionaries
            
        Returns:
            List of sampled annotation dictionaries
        """
        if not annotations_data:
            return []
            
        # Use sample_size from base class
        sample_size = min(self.sample_size, len(annotations_data))
        
        # Random sampling
        sampled_annotations = random.sample(annotations_data, sample_size)
        
        return sampled_annotations 