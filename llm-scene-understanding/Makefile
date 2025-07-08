.PHONY: help install build wheel executable clean test

# Default target
help:
	@echo "Available commands:"
	@echo "  install     - Install dependencies with uv"
	@echo "  build       - Build wheel package with uv"
	@echo "  wheel       - Build wheel package (alias for build)"
	@echo "  executable  - Create simple single-file executable"
	@echo "  executable-dir   - Create directory-based executable (with data files)"
	@echo "  executable-single - Create single-file executable (with data files)"
	@echo "  executable-all    - Create both simple and spec-based executables"
	@echo "  clean       - Clean build artifacts"
	@echo "  test        - Run tests"
	@echo "  help        - Show this help message"

# Install dependencies
install:
	uv sync

# Build wheel package
build:
	uv build --wheel

# Alias for build
wheel: build

# Create standalone executable (single-file)
executable:
	@echo "Creating standalone executable..."
	pyinstaller --name=sceneformer --onefile src/scene_understanding/cli.py
	@echo "Executable created: dist/sceneformer"

# Create directory-based executable using spec file
executable-dir:
	@echo "Creating directory-based executable..."
	pyinstaller --clean --noconfirm cli.spec
	@echo "Directory-based executable created: dist/cli/"

# Create single-file executable using spec file
executable-single:
	@echo "Creating single-file executable..."
	pyinstaller --clean --noconfirm sceneformer.spec
	@echo "Single-file executable created: dist/sceneformer"

# Create both executables (simple + spec-based)
executable-all:
	@echo "Creating both executables..."
	pyinstaller --name=sceneformer --onefile src/scene_understanding/cli.py
	pyinstaller --clean --noconfirm cli.spec
	@echo "Executables created: dist/sceneformer and dist/cli/"

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	@echo "Build artifacts cleaned"

# Run tests
test:
	pytest

# Install the built wheel
install-wheel:
	uv pip install dist/scene_understanding-*.whl

# Uninstall the package
uninstall:
	uv pip uninstall scene-understanding -y

# Development setup
dev-setup: install
	@echo "Development environment setup complete"

# Full build process
all: clean install build executable
	@echo "Full build process complete" 