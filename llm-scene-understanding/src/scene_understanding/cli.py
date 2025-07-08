#!/usr/bin/env python3
"""
Sceneformer CLI - AI-powered scene understanding tool
"""

import click
import os
import json
import yaml
from pathlib import Path
from typing import Optional, List
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from scene_understanding.ontology_generator import FashionpediaOntologyGenerator
from scene_understanding.duckdb_loader import FashionpediaDuckDBLoader
from scene_understanding.loader_factory import FashionpediaLoaderFactory, LoadingStrategy

console = Console()

@click.group()
@click.version_option(version="0.1.0", prog_name="sceneformer-cli")
@click.option('-v', '--verbose', is_flag=True, help='Enable verbose logging')
@click.option('-c', '--config', type=click.Path(exists=True), help='Configuration file path')
@click.pass_context
def main(ctx, verbose: bool, config: Optional[str]):
    """
    Sceneformer CLI - AI-powered scene understanding tool.
    
    Analyze trends from multiple data sources and generate insights with AI.
    """
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['config'] = config
    
    if verbose:
        console.print("[bold blue]Sceneformer CLI[/bold blue] - Verbose mode enabled")

@main.group()
@click.option('--ds', '--datasource', multiple=True, help='Data sources to analyze (streetstyle, event, etc.)')
@click.option('--lang', '--language', type=click.Choice(['en', 'ja']), default='en', help='Language for analysis')
@click.option('--mt', '--media-type', type=click.Choice(['image', 'text', 'both']), default='both', help='Media type to analyze')
@click.option('--kb', '--knowledgebase', type=click.Path(exists=True), help='Knowledge base path')
@click.pass_context
def input(ctx, datasource: tuple, language: str, media_type: str, knowledgebase: Optional[str]):
    """
    Input data analysis commands.
    
    Configure and process input data from various sources.
    """
    ctx.obj['datasources'] = list(datasource) if datasource else ['streetstyle', 'event']
    ctx.obj['language'] = language
    ctx.obj['media_type'] = media_type
    ctx.obj['knowledgebase'] = knowledgebase

@input.command()
@click.option('--taxonomy', is_flag=True, help='Include taxonomy analysis')
@click.option('--image-style', is_flag=True, help='Include image style analysis')
@click.option('--image', is_flag=True, help='Include image analysis')
@click.pass_context
def knowledgebase(ctx, taxonomy: bool, image_style: bool, image: bool):
    """
    Configure knowledge base settings.
    """
    kb_config = {
        'taxonomy': taxonomy,
        'image_style': image_style,
        'image': image
    }
    
    if ctx.obj['verbose']:
        console.print(f"[green]Knowledge base configuration:[/green] {kb_config}")
    
    # Save knowledge base configuration
    config_path = Path('.sceneformer.md')
    with open(config_path, 'w') as f:
        yaml.dump({'knowledgebase': kb_config}, f)
    
    console.print(f"[green]Knowledge base configuration saved to {config_path}[/green]")

@main.group()
@click.pass_context
def conf(ctx):
    """
    Configuration management commands.
    """
    pass

@conf.command()
@click.option('--init', is_flag=True, help='Initialize configuration files')
@click.pass_context
def setup(ctx, init: bool):
    """
    Setup Sceneformer configuration files.
    """
    if init:
        # Create .sceneformer.md
        sceneformer_config = {
            'version': '0.1.0',
            'datasources': ['streetstyle', 'event'],
            'language': 'en',
            'media_type': 'both',
            'output_formats': ['excel', 'owl', 'rdf', 'duckdb', 'duckdb_parquet', 'sqlite']
        }
        
        with open('.sceneformer.md', 'w') as f:
            yaml.dump(sceneformer_config, f)
        
        # Create sfscript template
        sfscript_template = {
            'analysis_type': 'trend',
            'filters': {},
            'outputs': ['taxonomy', 'tags']
        }
        
        with open('sfscript.yaml', 'w') as f:
            yaml.dump(sfscript_template, f)
        
        console.print("[green]Configuration files initialized successfully![/green]")
        console.print("[dim]Created: .sceneformer.md, sfscript.yaml[/dim]")

@main.group()
@click.pass_context
def output(ctx):
    """
    Output generation commands.
    """
    pass

@output.command()
@click.option('--format', 'output_format', type=click.Choice(['excel', 'owl', 'rdf', 'duckdb', 'duckdb_parquet', 'sqlite']), default='excel', help='Output format')
@click.option('--output-dir', '-o', type=click.Path(), default='./output', help='Output directory')
@click.option('--input', '-i', type=click.Path(exists=True), help='Input categories JSON file')
@click.option('--sample-size', type=int, default=1000, help='Sample size for large datasets (default: 1000)')
@click.option('--enable-sampling/--no-enable-sampling', default=True, help='Enable/disable sampling mode for datasets > 10,000 entries (default: enabled)')
@click.option('--small-dataset-mode/--no-small-dataset-mode', default=False, help='Use reduced sampling numbers for faster processing (100 images, 200 annotations)')
@click.option('--no-cleanup', is_flag=True, default=False, help='Keep intermediate Parquet files (for duckdb_parquet format)')
@click.pass_context
def taxonomy(ctx, output_format: str, output_dir: str, input: Optional[str], sample_size: int, enable_sampling: bool, small_dataset_mode: bool, no_cleanup: bool):
    """
    Generate taxonomy analysis output.
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    console.print(f"[bold blue]Generating taxonomy in {output_format} format...[/bold blue]")
    
    # Debug: Show received parameters
    console.print(f"[dim]DEBUG: sample_size={sample_size}, enable_sampling={enable_sampling}, small_dataset_mode={small_dataset_mode}[/dim]")
    
    if output_format == 'excel':
        # TODO: Generate Excel taxonomy
        console.print("[yellow]Excel taxonomy generation coming soon...[/yellow]")
    elif output_format in ['duckdb', 'duckdb_parquet', 'sqlite']:
        # Load Fashionpedia data into database
        input_path = Path("data/01_raw/instances_attributes_train2020.json")
        schema_path = Path("data/01_raw/fashionpedia_schema.sql")
        
        if output_format == 'sqlite':
            db_path = output_path / "fashionpedia.db"
        else:
            db_path = output_path / "fashionpedia.ddb"
        
        if not input_path.exists():
            console.print(f"[red]Error: Fashionpedia data not found at {input_path}[/red]")
            return
            
        if not schema_path.exists():
            console.print(f"[red]Error: Schema file not found at {schema_path}[/red]")
            return
            
        # Choose loading strategy
        if output_format == 'duckdb':
            strategy = LoadingStrategy.DIRECT_JSON
            strategy_name = "Direct JSON (DuckDB)"
        elif output_format == 'duckdb_parquet':
            strategy = LoadingStrategy.PARQUET_CONVERSION
            strategy_name = "Parquet Conversion (DuckDB)"
        else:  # sqlite
            strategy = LoadingStrategy.SQLITE_DIRECT
            strategy_name = "Direct JSON (SQLite)"
            
        try:
            # Show strategy information
            console.print(f"[bold blue]Using {strategy_name} strategy[/bold blue]")
            description = FashionpediaLoaderFactory.get_strategy_description(strategy)
            console.print(f"[dim]{description}[/dim]")
            
            # Show sampling information
            if enable_sampling:
                if small_dataset_mode:
                    console.print(f"[dim]Sampling mode: Enabled (small dataset mode: 100 images, 200 annotations)[/dim]")
                else:
                    console.print(f"[dim]Sampling mode: Enabled (sample size: {sample_size:,})[/dim]")
            else:
                console.print("[dim]Sampling mode: Disabled[/dim]")
            
            # Get strategy benefits
            benefits = FashionpediaLoaderFactory.get_strategy_benefits(strategy)
            if benefits:
                console.print("[dim]Benefits:[/dim]")
                for benefit in benefits[:3]:  # Show first 3 benefits
                    console.print(f"[dim]  • {benefit}[/dim]")
            
            # Initialize loader using factory
            console.print(f"[dim]DEBUG: Creating loader with sample_size={sample_size}, enable_sampling={enable_sampling}, small_dataset_mode={small_dataset_mode}[/dim]")
            
            # Pass additional kwargs for parquet loader
            loader_kwargs = {
                'sample_size': sample_size,
                'enable_sampling': enable_sampling,
                'small_dataset_mode': small_dataset_mode
            }
            
            # Add no_cleanup option for parquet loader
            if output_format == 'duckdb_parquet':
                loader_kwargs['no_cleanup'] = no_cleanup
                if no_cleanup:
                    console.print(f"[dim]Parquet cleanup disabled - intermediate files will be kept[/dim]")
                else:
                    console.print(f"[dim]Parquet cleanup enabled - intermediate files will be removed[/dim]")
            
            with FashionpediaLoaderFactory.create_loader(strategy, db_path, **loader_kwargs) as loader:
                # Create schema
                console.print("[bold blue]Creating database schema...[/bold blue]")
                loader.create_schema(schema_path)
                
                # Load data
                console.print("[bold blue]Loading Fashionpedia data...[/bold blue]")
                loader.load_json_data(input_path)
                
                # Get statistics
                stats = loader.get_database_stats()
                console.print(f"[green]✓ Database created: {db_path}[/green]")
                console.print(f"[dim]Total annotations: {stats['total_annotations']:,}[/dim]")
                console.print(f"[dim]Total images: {stats['total_images']:,}[/dim]")
                console.print(f"[dim]Total categories: {stats['total_categories']}[/dim]")
                console.print(f"[dim]Total attributes: {stats['total_attributes']}[/dim]")
                
                # Run sample queries if verbose
                if ctx.obj['verbose']:
                    loader.run_sample_queries()
                    
        except Exception as e:
            console.print(f"[red]Error loading data: {e}[/red]")
            if ctx.obj['verbose']:
                import traceback
                console.print(f"[red]{traceback.format_exc()}[/red]")
    elif output_format == 'owl':
        # Generate OWL ontology
        input_path = Path("data/01_raw/instances_attributes_train2020.categories.jsonl")
        output_file = output_path / "fashionpedia_ontology.owl"
        
        if not input_path.exists():
            console.print(f"[red]Error: Categories file not found at {input_path}[/red]")
            return
            
        try:
            generator = FashionpediaOntologyGenerator()
            graph = generator.generate_from_file(input_path, output_file)
            
            console.print(f"[green]✓ OWL ontology generated: {output_file}[/green]")
            
            # Show ontology summary
            _show_ontology_summary(graph)
            
        except Exception as e:
            console.print(f"[red]Error generating ontology: {e}[/red]")
            if ctx.obj['verbose']:
                import traceback
                console.print(f"[red]{traceback.format_exc()}[/red]")

def _show_ontology_summary(graph):
    """Show summary of generated ontology"""
    console.print("\n[bold blue]Ontology Summary:[/bold blue]")
    
    # Count classes (OWL.Class)
    classes = list(graph.subjects(predicate=graph.namespace_manager.namespaces()[0][1].type))
    console.print(f"[dim]Classes: {len(classes)}[/dim]")
    
    # Count object properties (OWL.ObjectProperty)
    object_properties = list(graph.subjects(predicate=graph.namespace_manager.namespaces()[0][1].type))
    console.print(f"[dim]Object Properties: {len(object_properties)}[/dim]")
    
    # Count data properties (OWL.DatatypeProperty)
    data_properties = list(graph.subjects(predicate=graph.namespace_manager.namespaces()[0][1].type))
    console.print(f"[dim]Data Properties: {len(data_properties)}[/dim]")
    
    # Show some example classes
    console.print("\n[cyan]Example Classes:[/cyan]")
    for i, cls in enumerate(classes[:5]):
        console.print(f"  • {cls.split('#')[-1] if '#' in str(cls) else str(cls)}")
    if len(classes) > 5:
        console.print(f"  ... and {len(classes) - 5} more")

@output.command()
@click.option('--input', '-i', type=click.Path(exists=True), required=True, help='Input image or text file')
@click.option('--output-dir', '-o', type=click.Path(), default='./output', help='Output directory')
@click.pass_context
def tag(ctx, input_path: str, output_dir: str):
    """
    Generate tags for input data.
    """
    console.print(f"[bold blue]Generating tags for {input_path}...[/bold blue]")
    # TODO: Implement tag generation
    console.print("[yellow]Tag generation coming soon...[/yellow]")

@main.command()
@click.option('--datasource', '-ds', multiple=True, help='Data sources to analyze')
@click.option('--lang', type=click.Choice(['en', 'ja']), default='en', help='Language')
@click.option('--media-type', '-mt', type=click.Choice(['image', 'text', 'both']), default='both', help='Media type')
@click.option('--output-format', '-f', type=click.Choice(['excel', 'owl', 'rdf', 'duckdb', 'duckdb_parquet', 'sqlite']), default='excel', help='Output format')
@click.option('--output-dir', '-o', type=click.Path(), default='./output', help='Output directory')
@click.option('--sample-size', type=int, default=1000, help='Sample size for large datasets (default: 1000)')
@click.option('--enable-sampling', is_flag=True, default=True, help='Enable sampling mode for datasets > 10,000 entries (default: enabled)')
@click.pass_context
def analyze(ctx, datasource: tuple, lang: str, media_type: str, output_format: str, output_dir: str, sample_size: int, enable_sampling: bool):
    """
    Analyze data and generate insights.
    """
    console.print("[bold blue]Sceneformer Analysis[/bold blue]")
    console.print(f"[dim]Data sources: {list(datasource) if datasource else ['streetstyle', 'event']}[/dim]")
    console.print(f"[dim]Language: {lang}[/dim]")
    console.print(f"[dim]Media type: {media_type}[/dim]")
    console.print(f"[dim]Output format: {output_format}[/dim]")
    console.print(f"[dim]Output directory: {output_dir}[/dim]")
    
    if enable_sampling:
        console.print(f"[dim]Sampling mode: Enabled (sample size: {sample_size:,})[/dim]")
    else:
        console.print("[dim]Sampling mode: Disabled[/dim]")
    
    # TODO: Implement analysis logic
    console.print("[yellow]Analysis functionality coming soon...[/yellow]")

@main.command()
@click.pass_context
def status(ctx):
    """
    Show system status and configuration.
    """
    console.print("[bold blue]Sceneformer CLI Status[/bold blue]")
    
    # Check configuration files
    config_files = ['.sceneformer.md', 'sfscript.yaml']
    for config_file in config_files:
        if Path(config_file).exists():
            console.print(f"[green]✓ {config_file}[/green]")
        else:
            console.print(f"[red]✗ {config_file}[/red]")
    
    # Check data files
    data_files = [
        'data/01_raw/instances_attributes_train2020.json',
        'data/01_raw/instances_attributes_train2020.categories.jsonl',
        'data/01_raw/fashionpedia_schema.sql'
    ]
    
    console.print("\n[bold blue]Data Files:[/bold blue]")
    for data_file in data_files:
        if Path(data_file).exists():
            size = Path(data_file).stat().st_size
            size_mb = size / (1024 * 1024)
            console.print(f"[green]✓ {data_file} ({size_mb:.1f} MB)[/green]")
        else:
            console.print(f"[red]✗ {data_file}[/red]")
    
    # Show available strategies
    console.print("\n[bold blue]Available Loading Strategies:[/bold blue]")
    recommendations = FashionpediaLoaderFactory.get_strategy_recommendations()
    for strategy in LoadingStrategy:
        description = FashionpediaLoaderFactory.get_strategy_description(strategy)
        console.print(f"[cyan]{strategy.value}:[/cyan] {description}")
        
        if strategy in recommendations:
            best_for = recommendations[strategy]['best_for']
            console.print(f"[dim]  Best for: {', '.join(best_for[:2])}...[/dim]")

if __name__ == '__main__':
    main() 