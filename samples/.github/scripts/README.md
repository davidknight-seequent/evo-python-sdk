# Notebook Testing Scripts

This directory contains scripts and configuration for automated testing of Jupyter notebooks in the Evo Python SDK samples.

## Location

These scripts are located in `samples/.github/` to ensure they have access to the samples virtual environment, which contains all the necessary dependencies for running the notebooks.

## Files

### `.env.example`
Template file for local testing configuration. Copy this to `.env` and fill in your actual credentials.

**Required variables:**
- `EVO_CLIENT_ID` - Service app client ID
- `EVO_CLIENT_SECRET` - Service app client secret  
- `EVO_ORG_ID` - Organization ID
- `EVO_HUB_URL` - API hub URL (e.g., `au.api.seequent.com`)

**Optional variables:**
- `EVO_WORKSPACE_ID` - Use an existing workspace (if your service app can't create workspaces)

**Workspace Behavior:**
- If `EVO_WORKSPACE_ID` is **not set**: The script will attempt to create temporary workspaces with names like `test-{notebook-name}-{timestamp}` and clean them up after testing
- If `EVO_WORKSPACE_ID` is **set**: The script will use the existing workspace (and won't delete it)

### `test_notebooks.py`
Main test script that:
1. Finds all Jupyter notebooks in the parent `samples/` directory
2. Creates a temporary test workspace for each notebook (or uses existing workspace if `EVO_WORKSPACE_ID` is set)
3. Modifies them to use service app authentication (preserving original imports)
4. Executes all cells using the workspace
5. Cleans up by deleting the test workspace (only if it was created by the script)
6. Reports success/failure for each notebook

**Note**: This script uses `Path(__file__).parent.parent` to locate the samples directory relative to its location in `samples/.github/scripts/`.

**Key Features:**
- **Import Preservation**: Extracts and preserves imports from the original notebook's auth cell
- **Service App Auth**: Injects OAuth client credentials flow
- **Debug Mode**: Saves modified notebooks to `test-results/modified-notebooks/` for troubleshooting
- **Timeout Handling**: 30-second timeouts on all HTTP requests

## How It Works

### Notebook Modification Process

When a notebook is tested, the script:

1. **Finds the auth cell**: Locates the first code cell containing `client_id`, `redirect_url`, `ServiceManagerWidget`, or `manager`

2. **Extracts imports**: Parses the original cell and extracts:
   - All `import` and `from ... import ...` statements
   - Variable assignments like `cache_location` and `input_path`
   - Stops extraction when auth-related code is encountered

3. **Builds new auth cell**: Creates a replacement cell with:
   - Service app authentication code
   - All preserved imports from the original cell
   - Environment configuration using test credentials

4. **Replaces the cell**: Swaps the original auth cell with the new one

### Example Transformation

**Original Cell:**
```python
import uuid
import pandas as pd
from evo_schemas.components import BoundingBox_V1_0_1
from IPython.display import HTML, display
from evo.objects import ObjectAPIClient

cache_location = "data"
input_path = f"{cache_location}/input"

client_id = "daves-evo-client"
redirect_url = "http://localhost:32369/auth/callback"

manager = await ServiceManagerWidget.with_auth_code(...)
```

**Modified Cell:**
```python
# CI Test Setup - Service App Authentication
import uuid
from evo.aio import AioTransport
from evo.oauth import ClientCredentialsAuthorizer, EvoScopes, OAuthConnector
from evo.common import APIConnector, Environment

import uuid
import pandas as pd
from evo_schemas.components import BoundingBox_V1_0_1
from IPython.display import HTML, display
from evo.objects import ObjectAPIClient

cache_location = "data"
input_path = f"{cache_location}/input"

# Environment variables from CI
org_id = "..."
workspace_id = "..."
client_id = "..."
...
```

## Local Testing

1. **Install dependencies:**
   ```bash
   cd samples
   uv sync --extra testing
   ```

2. **Set up environment:**
   ```bash
   cp samples/.github/scripts/.env.example samples/.github/scripts/.env
   # Edit .env with your actual credentials
   ```

3. **Run tests:**
   ```bash
   cd samples
   uv run python .github/scripts/test_notebooks.py
   ```

## Debugging

If tests fail, check the modified notebooks in `samples/test-results/modified-notebooks/` to see:
- What imports were preserved
- How the auth cell was constructed
- If any code was incorrectly modified

Common issues:
- **Missing imports**: If a notebook fails with "NameError: name 'X' is not defined", the import extraction may need adjustment
- **Wrong workspace**: Ensure `EVO_WORKSPACE_ID` points to a workspace your service app can access
- **Auth errors**: Verify service app credentials have the correct scopes (evo_discovery, evo_workspace, evo_object)
