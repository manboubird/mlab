# Sceneformer CLI

AI-powered scene understanding tool for statistical analysis from multiple data sources and generating insights with AI.

## Features

- **Multi-source Data Analysis**: Analyze scene of image in data sources
- **Multi-language Support**: English and Japanese analysis
- **Multi-media Analysis**: Image and text content analysis
- **AI-powered Insights**: Generate taxonomy, tags, and insights using LLMs
- **Multiple Output Formats**: Excel, OWL/RDF, and custom formats
- **Configuration Management**: Flexible configuration system

## Installation

### Prerequisites

- Python 3.13 or higher
- uv package manager (recommended)

### Install with uv

```bash
# Clone the repository
git clone <repository-url>
cd scene-understanding

# Install dependencies
uv sync

# Install the CLI
uv pip install -e .
```

### Install with pip

```bash
pip install -e .
```

### Create Standalone Executable

You can create a standalone executable file using PyInstaller:

```bash
# Install PyInstaller
uv add --dev pyinstaller

# Create standalone executable
pyinstaller --name=sceneformer --onefile src/scene_understanding/cli.py

# The executable will be created in the dist/ directory
# You can run it directly:
./dist/sceneformer --version
./dist/sceneformer --help
```

**PyInstaller Options:**
- `--name=sceneformer` - Sets the name of the executable
- `--onefile` - Creates a single executable file

### Using Makefile

For convenience, you can use the provided Makefile for common tasks:

```bash
# Show available commands
make help

# Install dependencies
make install

# Build wheel package
make build
# or
make wheel

# Create standalone executable
make executable

# Clean build artifacts
make clean

# Run tests
make test

# Full build process (clean, install, build, executable)
make all

# Install the built wheel
make install-wheel

# Uninstall the package
make uninstall
```

### Build and Install Wheel Package

#### Build Wheel Package with uv

```bash
# Build wheel package
uv build

# This creates a .whl file in the dist/ directory
# Example: dist/scene_understanding-0.1.0-py3-none-any.whl
```

#### Install from Local Wheel File

```bash
# Install from the built wheel file
pip install dist/scene_understanding-0.1.0-py3-none-any.whl

# Or install with uv
uv pip install dist/scene_understanding-0.1.0-py3-none-any.whl

# After installation, you can use the CLI directly
sceneformer-cli --version
```

## Building and Testing

### Build Wheel Package

```bash
# Build the wheel package
uv build --wheel

# Check the generated wheel file
ls -la dist/

# The wheel file will be named something like:
# scene_understanding-0.1.0-py3-none-any.whl
```

### Test Wheel Package Installation

```bash
# Install the wheel package
uv pip install dist/scene_understanding-0.1.0-py3-none-any.whl

# Test the installed CLI
sceneformer-cli --version
sceneformer-cli --help

# Uninstall if needed
uv pip uninstall scene-understanding
```

## Running Without Installation

You can run the CLI directly without installing it by using Python:

```bash
# Show help
uv run python src/scene_understanding/cli.py --help

# Show version
uv run python src/scene_understanding/cli.py --version

# Check status
uv run python src/scene_understanding/cli.py status

# Initialize configuration
uv run python src/scene_understanding/cli.py conf setup --init



# Run analysis
uv run python src/scene_understanding/cli.py analyze --datasource streetstyle event --lang en
```

### Quick Test Commands

Try these commands to test the CLI functionality:

```bash
# Test 1: Check if CLI works
uv run python src/scene_understanding/cli.py --version

# Test 2: Show help
uv run python src/scene_understanding/cli.py --help

# Test 3: Check status (will show missing config files)
uv run python src/scene_understanding/cli.py status

# Test 4: Initialize configuration
uv run python src/scene_understanding/cli.py conf setup --init

# Test 5: Check status again (will show found config files)
uv run python src/scene_understanding/cli.py status



# Test 7: Set up knowledge base
uv run python src/scene_understanding/cli.py input knowledgebase --taxonomy --image-style

# Test 8: Run a simple analysis
uv run python src/scene_understanding/cli.py analyze --datasource streetstyle --lang en --media-type text
```

### Executing sceneformer-cli with Parameters

After building and installing the wheel package, you can execute the CLI with various parameters:

```bash
# Basic commands with parameters
sceneformer-cli --version
sceneformer-cli --help
sceneformer-cli --verbose status

# Input configuration with parameters
sceneformer-cli input --ds streetstyle event --lang ja --mt image
sceneformer-cli input knowledgebase --taxonomy --image-style --image

# Configuration with parameters
sceneformer-cli conf setup --init

# Output generation with parameters
sceneformer-cli output taxonomy --format owl --output-dir ./taxonomy_results
sceneformer-cli output tag --input ./sample_image.jpg --output-dir ./tagged_content

# Analysis with parameters
sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both --output-format excel --output-dir ./analysis_results

# Verbose mode with parameters
sceneformer-cli --verbose analyze --datasource event --lang ja --media-type image
```

## Quick Start

### 1. Initialize Configuration

```bash
# Using the installed CLI
sceneformer-cli conf setup --init

# Or run directly with Python (without installation)
python src/llm_scene_understanding/cli.py conf setup --init
```

This creates the following configuration files:
- `.sceneformer.md` - Main configuration
- `sfscript.yaml` - Analysis scripts

### 2. Run Analysis

```bash
# Using the installed CLI
sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both --output-format excel

# Or run directly with Python (without installation)
uv run python src/scene_understanding/cli.py analyze --datasource streetstyle event --lang en --media-type both --output-format excel
```

## CLI Commands

### Main Commands

```bash
# Using installed CLI
sceneformer-cli [OPTIONS] COMMAND [ARGS]...

# Or run directly with Python
uv run python src/llm_scene_understanding/cli.py [OPTIONS] COMMAND [ARGS]...
```

**Global Options:**
- `-v, --verbose` - Enable verbose logging
- `-c, --config` - Configuration file path
- `--version` - Show version information
- `--help` - Show help message

### Command Reference

```bash
# Main commands
sceneformer-cli --help                    # Show main help
sceneformer-cli --version                 # Show version
sceneformer-cli status                    # Show configuration status

# Input commands
sceneformer-cli input --help              # Show input help
sceneformer-cli input --ds streetstyle event  # Set data sources
sceneformer-cli input --lang ja           # Set language to Japanese
sceneformer-cli input --mt image          # Set media type to image only
sceneformer-cli input knowledgebase --taxonomy --image-style --image

# Configuration commands
sceneformer-cli conf --help               # Show configuration help
sceneformer-cli conf setup --init         # Initialize configuration files

# Output commands
sceneformer-cli output --help             # Show output help
sceneformer-cli output taxonomy --format excel --output-dir ./results
sceneformer-cli output tag --input ./image.jpg --output-dir ./tags

# Analysis commands
sceneformer-cli analyze --help            # Show analysis help
sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both --output-format excel
```

### Input Commands

#### Configure Data Sources

```bash
sceneformer-cli input [OPTIONS] COMMAND [ARGS]...
```

**Options:**
- `--ds, --datasource` - Data sources to analyze (streetstyle, event, etc.)
- `--lang, --language` - Language for analysis (en, ja)
- `--mt, --media-type` - Media type to analyze (image, text, both)
- `--kb, --knowledgebase` - Knowledge base path

#### Knowledge Base Configuration

```bash
sceneformer-cli input knowledgebase [OPTIONS]
```

**Options:**
- `--taxonomy` - Include taxonomy analysis
- `--image-style` - Include image style analysis
- `--image` - Include image analysis



### Configuration Commands

#### Setup Configuration

```bash
sceneformer-cli conf setup [OPTIONS]
```

**Options:**
- `--init` - Initialize configuration files



### Output Commands

#### Generate Taxonomy

```bash
sceneformer-cli output taxonomy [OPTIONS]
```

**Options:**
- `--format` - Output format (excel, owl, rdf)
- `--output-dir, -o` - Output directory

#### Generate Tags

```bash
sceneformer-cli output tag [OPTIONS] --input INPUT
```

**Options:**
- `--input, -i` - Input image or text file (required)
- `--output-dir, -o` - Output directory

### Analysis Commands

#### Complete Analysis Workflow

```bash
sceneformer-cli analyze [OPTIONS]
```

**Options:**
- `--datasource, -ds` - Data sources to analyze
- `--lang` - Language (en, ja)
- `--media-type, -mt` - Media type (image, text, both)
- `--output-format, -f` - Output format (excel, owl, rdf)
- `--output-dir, -o` - Output directory

#### Status Check

```bash
sceneformer-cli status
```

Shows current configuration status and settings.

## Usage Examples

### Basic Trend Analysis

```bash
# Analyze SNS and e-commerce data in English
# Using the installed CLI
sceneformer-cli analyze --datasource streetstyle event --lang en --media-type both

# Or run directly with Python (without installation)
uv run python src/scene_understanding/cli.py analyze --datasource streetstyle event --lang en --media-type both

# Generate taxonomy in Excel format
# Using the installed CLI
sceneformer-cli output taxonomy --format excel --output-dir ./results

# Or run directly with Python (without installation)
uv run python src/scene_understanding/cli.py output taxonomy --format excel --output-dir ./results

# Tag an image file
# Using the installed CLI
sceneformer-cli output tag --input ./images/streetstyle.jpg --output-dir ./tags

# Or run directly with Python (without installation)
uv run python src/scene_understanding/cli.py output tag --input ./images/streetstyle.jpg --output-dir ./tags
```

### Advanced Configuration

```bash
# Initialize with custom settings
# Using the installed CLI
sceneformer-cli conf setup --init

# Or run directly with Python (without installation)
python src/llm_scene_understanding/cli.py conf setup --init



# Set up knowledge base with taxonomy
# Using the installed CLI
sceneformer-cli input knowledgebase --taxonomy --image-style --image

# Or run directly with Python (without installation)
uv run python src/llm_scene_understanding/cli.py input knowledgebase --taxonomy --image-style --image
```

### Multi-language Analysis

```bash
# Japanese analysis
# Using the installed CLI
sceneformer-cli analyze --datasource streetstyle event --lang ja --media-type image

# Or run directly with Python (without installation)
uv run python src/scene_understanding/cli.py analyze --datasource streetstyle event --lang ja --media-type image

# Generate Japanese taxonomy
# Using the installed CLI
sceneformer-cli output taxonomy --format excel --output-dir ./japanese_results

# Or run directly with Python (without installation)
uv run python src/scene_understanding/cli.py output taxonomy --format excel --output-dir ./japanese_results
```

## Configuration Files

### .sceneformer.md
Main configuration file containing:
- Version information
- Data source settings
- Language preferences
- Media type settings
- Output format preferences

### sfscript.yaml
Analysis script configuration:
- Analysis type
- Filters
- Output specifications


## Data Sources

Supported data sources include:
- **Streetstyle**: Street fashion and style data
- **Event**: Event and gathering data
- **Custom**: User-defined data sources

## Output Formats

### Taxonomy Outputs
- **Excel**: Spreadsheet format with multiple sheets
- **OWL**: Web Ontology Language format
- **RDF**: Resource Description Framework format

### Tag Outputs
- **JSON**: Structured tag data
- **CSV**: Comma-separated values

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

### Adding New Commands

To add new commands, extend the CLI in `src/scene_understanding/cli.py`:

```python
@main.command()
@click.option('--option', help='Option description')
def new_command(option):
    """New command description."""
    # Implementation here
    pass
``` 

## References

- [receptron/mulmocast\-cli: AI\-powered podcast & video generator\.](https://github.com/receptron/mulmocast-cli)