# -*- mode: python ; coding: utf-8 -*-

# Import required modules for data collection
import os
from pathlib import Path

# Get the project root directory
project_root = Path(os.getcwd())

# Collect data files
data_files = []

# Add schema files
schema_dir = project_root / "data" / "01_raw"
if schema_dir.exists():
    for schema_file in schema_dir.glob("*.sql"):
        data_files.append((str(schema_file), "data/01_raw"))

# Add sample data files (if they exist)
sample_data_dir = project_root / "data" / "01_raw"
if sample_data_dir.exists():
    for data_file in sample_data_dir.glob("*.json*"):
        if data_file.name.startswith("instances_attributes"):
            data_files.append((str(data_file), "data/01_raw"))

# Add configuration files
config_files = [
    (str(project_root / "pyproject.toml"), "."),
    (str(project_root / "README.md"), "."),
    (str(project_root / "Makefile"), "."),
]

# Add all data files
data_files.extend(config_files)

# Collect hidden imports
hidden_imports = [
    # Core modules
    'src.scene_understanding',
    'src.scene_understanding.cli',
    'src.scene_understanding.config',
    'src.scene_understanding.types',
    'src.scene_understanding.utils',
    'src.scene_understanding.utils.logging',
    
    # Loader modules
    'src.scene_understanding.base_loader',
    'src.scene_understanding.duckdb_loader',
    'src.scene_understanding.sqlite_loader',
    'src.scene_understanding.parquet_loader',
    'src.scene_understanding.loader_factory',
    
    # Ontology modules
    'src.scene_understanding.ontology_generator',
    
    # Third-party dependencies
    'click',
    'rich',
    'rich.console',
    'rich.progress',
    'rich.table',
    'rich.panel',
    'rich.logging',
    'rich.traceback',
    'duckdb',
    'sqlite3',
    'rdflib',
    'rdflib.namespace',
    'rdflib.plugins.sparql',
    'rdflib.plugins.serializers.turtle',
    'rdflib.plugins.serializers.xmlwriter',
    'orjson',
    'yaml',
    'json',
    'pathlib',
    'typing',
    'logging',
    'random',
    'time',
    'shutil',
    'tempfile',
    'enum',
    'dataclasses',
    'abc',
    'contextlib',
]

# Analysis configuration
a = Analysis(
    ['src/scene_understanding/cli.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=data_files,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'sklearn',
        'tensorflow',
        'torch',
        'jupyter',
        'IPython',
        'notebook',
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='cli',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='cli',
)
