# Notebook Testing Infrastructure

This directory contains the testing infrastructure for Jupyter notebooks in the Evo Python SDK samples.

## Directory Structure

```
samples/
├── .github/
│   └── scripts/
│       ├── test_notebooks.py    # Main test script
│       ├── .env.example         # Environment variable template
│       └── README.md            # Detailed documentation
├── geoscience-objects/          # Sample notebooks
├── blockmodels/
├── files/
└── ...
```

The workflow configuration is in the root `.github/workflows/` directory (required by GitHub Actions):
```
.github/
└── workflows/
    ├── test-notebooks.yaml      # GitHub Actions workflow
    └── NOTEBOOK_TESTING.md      # Workflow documentation
```

## Why This Location?

The test scripts are in `samples/.github/` (not the root `.github/`) because:

1. **Virtual Environment Access**: Notebooks require the `samples` virtual environment with all Evo SDK dependencies installed
2. **Relative Path Resolution**: The script uses `Path(__file__).parent.parent` to locate the samples directory
3. **Dependency Isolation**: Keeps testing dependencies (nbformat, nbclient, etc.) within the samples environment

## Quick Start

1. **Set up environment:**
   ```bash
   cd samples
   cp .github/scripts/.env.example .github/scripts/.env
   # Edit .env with your credentials
   ```

2. **Install dependencies:**
   ```bash
   uv sync --extra testing
   ```

3. **Run tests:**
   ```bash
   uv run python .github/scripts/test_notebooks.py
   ```

## How It Works

The test script:
1. Locates notebooks in the samples directory using relative paths
2. Extracts and preserves imports from original auth cells
3. Injects service app authentication code
4. Executes notebooks using the samples virtual environment
5. Saves modified notebooks to `test-results/modified-notebooks/` for debugging

See `scripts/README.md` for detailed documentation.

## GitHub Actions Integration

The workflow at `.github/workflows/test-notebooks.yaml`:
1. Checks out the repository
2. Sets up Python and installs uv
3. Changes to `samples/` directory
4. Installs dependencies with `uv sync`
5. Runs tests with `cd samples && uv run python .github/scripts/test_notebooks.py`
6. Uploads test results as artifacts

The workflow must be in the root `.github/workflows/` directory (GitHub requirement), but it executes commands from the `samples/` directory to access the correct virtual environment.
