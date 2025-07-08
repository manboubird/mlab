# Sceneformer CLI

AI-powered scene understanding tool for statistical analysis and insights from multiple data sources.

---

## Table of Contents
- [Overview](#overview)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Build & Executable Options](#build--executable-options)
- [Usage](#usage)
- [Configuration](#configuration)
- [Command Reference](#command-reference)
- [Advanced Usage](#advanced-usage)
- [Development](#development)
- [References](#references)

---

## Overview

**Sceneformer** is a CLI tool for AI-powered scene understanding, supporting:
- Multi-source data analysis (e.g., streetstyle, event)
- Multi-language (English, Japanese)
- Multi-media (image, text)
- AI-driven taxonomy, tags, and insights (LLMs)
- Multiple output formats: Excel, OWL/RDF, DuckDB, SQLite
- Flexible configuration and sampling for large datasets
- Standalone executables (PyInstaller) and Python CLI

---

## Quick Start

```bash
# 1. Install dependencies
uv sync

# 2. Initialize configuration
make install
sceneformer-cli conf setup --init

# 3. Run a basic analysis
sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both --output-format excel
```

---

## Installation

### Prerequisites
- Python 3.13 or higher
- [uv](https://github.com/astral-sh/uv) (recommended)

### Install with uv
```bash
uv sync
uv pip install -e .
```

### Install with pip
```bash
pip install -e .
```

---

## Build & Executable Options

### Standalone Executables (PyInstaller)

#### Simple Single-file Executable (Recommended)
```bash
uv add --dev pyinstaller
make executable
# or
pyinstaller --name=sceneformer --onefile src/scene_understanding/cli.py
```
- Output: `dist/sceneformer`

#### Advanced Executables (with Data Files)
```bash
make executable-dir      # Directory-based (with data files)
make executable-single   # Single-file (with data files)
make executable-all      # Both
```
- Outputs: `dist/cli/`, `dist/sceneformer`

#### Manual PyInstaller
```bash
pyinstaller --name=sceneformer --onefile src/scene_understanding/cli.py
pyinstaller --clean --noconfirm cli.spec
pyinstaller --clean --noconfirm sceneformer.spec
```

#### Executable Differences
- **Simple single-file**: Fastest, no data files, good for most users
- **Directory-based**: Includes data files, better for dev
- **Advanced single-file**: Includes data files, portable, slower startup

---

## Usage

### CLI (installed)
```bash
sceneformer-cli --help
sceneformer-cli --version
sceneformer-cli status
```

### Python (no install)
```bash
uv run python src/scene_understanding/cli.py --help
```

### Example: Analyze Data
```bash
sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both --output-format excel
```

---

## Configuration

### Initialize
```bash
sceneformer-cli conf setup --init
```
- Creates `.sceneformer.md` and `sfscript.yaml`

### Customize
Edit `.sceneformer.md` for:
- Data sources
- Language
- Media type
- Output formats

---

## Command Reference

### Main
- `sceneformer-cli --help` — Show help
- `sceneformer-cli --version` — Show version
- `sceneformer-cli status` — Show config status

### Input
- `sceneformer-cli input --ds streetstyle event --lang ja --mt image`
- `sceneformer-cli input knowledgebase --taxonomy --image-style --image`

### Configuration
- `sceneformer-cli conf setup --init`

### Output
- `sceneformer-cli output taxonomy --format excel --output-dir ./results`
- `sceneformer-cli output tag --input ./image.jpg --output-dir ./tags`

### Analysis
- `sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both --output-format excel`

---

## Advanced Usage

### Sampling & Large Datasets
- `--enable-sampling` / `--no-enable-sampling`: Enable/disable sampling (default: enabled for >10,000 entries)
- `--sample-size <N>`: Set sample size (default: 1000)
- `--small-dataset-mode`: Use small sample sizes for quick tests

### Output Formats
- `excel`, `owl`, `rdf`, `duckdb`, `duckdb_parquet`, `sqlite`

### Example: DuckDB/SQLite with Sampling
```bash
sceneformer-cli output taxonomy --format duckdb --output-dir ./results --input data/01_raw/instances_attributes_train2020.json --sample-size 500
sceneformer-cli output taxonomy --format sqlite --output-dir ./results --input data/01_raw/instances_attributes_train2020.json --small-dataset-mode
```

---

## Development

### Project Structure
```
scene-understanding/
├── src/
│   └── scene_understanding/
│       ├── __init__.py
│       └── cli.py
├── pyproject.toml
├── README.md
└── uv.lock
```

### Add New Commands
Edit `src/scene_understanding/cli.py`:
```python
@main.command()
@click.option('--option', help='Option description')
def new_command(option):
    """New command description."""
    pass
```

---

## References
- [receptron/mulmocast-cli](https://github.com/receptron/mulmocast-cli)