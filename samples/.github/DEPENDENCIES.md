# Testing Dependencies for Evo Python SDK Samples

## Overview

The samples package now includes optional testing dependencies that enable automated testing of Jupyter notebooks.

## Testing Dependencies

The following dependencies are installed when using the `testing` extra:

- **nbformat** (>=5.10.0) - Read and write Jupyter notebook files
- **nbclient** (>=0.10.0) - Execute Jupyter notebooks programmatically
- **ipykernel** (>=6.29.0) - IPython kernel for Jupyter
- **jupyter** (>=1.1.0) - Jupyter metapackage
- **python-dotenv** (>=1.0.0) - Load environment variables from .env files

## Installation

### For Regular Use
```bash
cd samples
uv sync
```

### For Testing/Development
```bash
cd samples
uv sync --extra testing
```

## Usage in pyproject.toml

```toml
[project.optional-dependencies]
testing = [
    "nbformat>=5.10.0",
    "nbclient>=0.10.0",
    "ipykernel>=6.29.0",
    "jupyter>=1.1.0",
    "python-dotenv>=1.0.0",
]
```

## Why Optional Dependencies?

- **Separation of Concerns**: Regular users of samples don't need testing tools
- **Faster Installation**: Default `uv sync` is faster without testing deps
- **Clean Environment**: Keeps the production environment minimal
- **CI/CD Ready**: GitHub Actions can install exactly what's needed

## GitHub Actions Integration

The workflow uses:
```yaml
- name: Install dependencies
  run: |
    cd samples
    uv sync --extra testing
```

This ensures the CI environment has all necessary dependencies to test notebooks.

## Local Development

When developing or debugging tests:

1. Install testing dependencies:
   ```bash
   cd samples
   uv sync --extra testing
   ```

2. Set up environment variables:
   ```bash
   cp .github/scripts/.env.example .github/scripts/.env
   # Edit .env with your credentials
   ```

3. Run tests:
   ```bash
   uv run python .github/scripts/test_notebooks.py
   ```

The `uv run` command automatically uses the samples virtual environment with all installed dependencies.
