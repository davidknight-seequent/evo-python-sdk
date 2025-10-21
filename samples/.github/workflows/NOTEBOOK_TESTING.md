# Notebook Testing Workflow

This GitHub Action automatically tests all Jupyter notebooks in the repository when a pull request is opened or updated.

## Overview

The workflow (`test-notebooks.yaml`) performs the following steps for each notebook:

1. **Setup**: Replaces the user authentication flow with service app authentication
2. **Workspace Usage**: Uses an existing workspace (specified via `EVO_WORKSPACE_ID`) for the notebook tests
3. **Execution**: Runs all cells in the notebook
4. **Reporting**: Generates a test report showing which notebooks passed/failed

**Note**: Unlike earlier versions, this workflow now uses an existing workspace instead of creating and deleting test workspaces. This avoids permission issues with workspace creation.

## Required GitHub Secrets

Before this workflow can run, you must configure the following secrets in your repository:

- `EVO_SERVICE_APP_CLIENT_ID`: Client ID for your Evo service application
- `EVO_SERVICE_APP_CLIENT_SECRET`: Client secret for your Evo service application  
- `EVO_ORG_ID`: Your Evo organization ID
- `EVO_HUB_URL`: Your Evo hub URL (e.g., `api.evo.seequent.com`)
- `EVO_WORKSPACE_ID`: ID of an existing workspace to use for testing (the service app must have access to this workspace)

### Setting up secrets

1. Go to your repository settings
2. Navigate to **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add each of the secrets listed above

## Skipped Notebooks

The following notebooks are automatically skipped as they are used for authentication setup or workspace management:

- `native-app-token.ipynb`
- `service-app-token.ipynb`
- `evo-discovery.ipynb`
- `create-a-workspace.ipynb`
- `delete-a-workspace.ipynb`

## Running Locally

You can also run the test script locally:

```bash
## Local Testing

To test the workflow locally:

1. **Install dependencies**:
   ```bash
   cd samples
   uv sync
   uv pip install nbformat nbclient ipykernel jupyter python-dotenv
   ```

2. **Create a `.env` file** in `samples/.github/scripts/` directory:
   ```bash
   cp samples/.github/scripts/.env.example samples/.github/scripts/.env
   ```

3. **Edit `.env`** with your actual credentials:
   ```
   EVO_CLIENT_ID=your-client-id
   EVO_CLIENT_SECRET=your-client-secret
   EVO_ORG_ID=your-org-id
   EVO_HUB_URL=your-evo-hub-url
   EVO_WORKSPACE_ID=your-workspace-id
   ```

4. **Run the test script**:
   ```bash
   cd samples
   uv run python .github/scripts/test_notebooks.py
   ```
```

## Test Results

After the workflow completes:

- A summary is printed showing passed/failed notebooks
- Detailed results are saved to `test-results/results.json`
- Test artifacts are uploaded and available for download from the workflow run

## Troubleshooting

### Notebook fails in CI but works locally

- Ensure the notebook doesn't rely on interactive input
- Check that all required files are committed to the repository
- Verify that relative file paths are correct

### Authentication errors

- Double-check that all GitHub secrets are correctly configured
- Ensure the service app has the necessary permissions
- Verify the org ID and hub URL are correct

### Timeout errors

- The default timeout is 600 seconds (10 minutes) per notebook
- For longer-running notebooks, adjust the `timeout` parameter in `test_notebooks.py`

## Contributing

When adding new notebooks:

1. Ensure they don't require interactive user input
2. Use relative paths for file references
3. Include proper error handling
4. Test locally before submitting a PR

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Pull Request Opened/Updated                                 │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ test-notebooks.yaml (GitHub Actions Workflow)               │
│  - Checks out code                                          │
│  - Sets up Python & uv                                      │
│  - Installs dependencies                                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ test_notebooks.py (Python Test Script)                      │
│                                                             │
│  For each notebook:                                         │
│   1. Load notebook                                          │
│   2. Replace auth cell with service app flow                │
│   3. Create test workspace                                  │
│   4. Execute notebook                                       │
│   5. Delete test workspace                                  │
│                                                             │
│  Generate test report                                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ Results                                                     │
│  - Console output with pass/fail status                     │
│  - test-results/results.json artifact                       │
│  - Workflow success/failure status                          │
└─────────────────────────────────────────────────────────────┘
```
