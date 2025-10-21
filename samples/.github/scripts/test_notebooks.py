#!/usr/bin/env python3
"""
Test script for Jupyter notebooks in the evo-python-sdk repository.

This script:
1. Finds all Jupyter notebooks in the samples directory
2. Modifies them to use service app authentication
3. Creates a test workspace
4. Runs the notebook
5. Cleans up by deleting the test workspace
"""

import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import List

import nbformat
import requests
from dotenv import load_dotenv
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

# Load environment variables from .env file if it exists (in same directory as this script)
# Use override=True to ensure .env values take precedence over shell environment
script_dir = Path(__file__).parent
load_dotenv(script_dir / ".env", override=True)


class NotebookTester:
    """Test runner for Jupyter notebooks with Evo workspace setup/teardown."""

    def __init__(self):
        self.client_id = os.getenv("EVO_CLIENT_ID")
        self.client_secret = os.getenv("EVO_CLIENT_SECRET")
        self.org_id = os.getenv("EVO_ORG_ID")
        self.hub_url = os.getenv("EVO_HUB_URL")
        self.workspace_id = os.getenv("EVO_WORKSPACE_ID")  # Optional: use existing workspace
        self.auth_token = None

        if not all([self.client_id, self.client_secret, self.org_id, self.hub_url]):
            raise ValueError(
                "Missing required environment variables: EVO_CLIENT_ID, EVO_CLIENT_SECRET, EVO_ORG_ID, EVO_HUB_URL"
            )

        # Print configuration for verification
        print("🔧 Configuration loaded:")
        print(f"   Organization ID: {self.org_id}")
        print(f"   Hub URL: {self.hub_url}")
        if self.workspace_id:
            print(f"   Workspace ID: {self.workspace_id} (from config)")
        else:
            print("   Workspace ID: Will be created per test")

    def get_auth_token(self) -> str:
        """Obtain an auth token using service app credentials."""
        if self.auth_token:
            return self.auth_token

        scope = "evo.workspace evo.discovery evo.object evo.blocksync evo.file"
        url = "https://ims.bentley.com/connect/token"

        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": scope,
        }

        response = requests.post(url, data=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to get token: {response.status_code} {response.reason}")

        self.auth_token = response.json()["access_token"]
        return self.auth_token

    def create_workspace(self, name: str) -> str:
        """Create a test workspace."""
        token = self.get_auth_token()

        payload = {
            "name": name,
            "description": "Automated test workspace - safe to delete",
            "labels": ["ci-test", "automated"],
        }

        response = requests.post(
            f"https://{self.hub_url}/workspace/orgs/{self.org_id}/workspaces",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

        if response.status_code == 201:
            self.workspace_id = response.json()["id"]
            print(f"✓ Created test workspace: {self.workspace_id}")
            return self.workspace_id
        elif response.status_code == 409:
            # Workspace already exists, try to get it
            print(f"⚠ Workspace '{name}' already exists, attempting cleanup...")
            self.delete_existing_workspace(name)
            time.sleep(2)
            return self.create_workspace(name)
        else:
            raise RuntimeError(f"Failed to create workspace: {response.status_code} {response.text}")

    def delete_workspace(self, workspace_id: str = None) -> None:
        """Delete a workspace."""
        workspace_id = workspace_id or self.workspace_id
        if not workspace_id:
            return

        token = self.get_auth_token()

        response = requests.delete(
            f"https://{self.hub_url}/workspace/orgs/{self.org_id}/workspaces/{workspace_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

        if response.status_code == 204:
            print(f"✓ Deleted workspace: {workspace_id}")
        else:
            print(f"⚠ Failed to delete workspace {workspace_id}: {response.status_code}")

    def delete_existing_workspace(self, name: str) -> None:
        """Find and delete workspace by name."""
        token = self.get_auth_token()

        response = requests.get(
            f"https://{self.hub_url}/workspace/orgs/{self.org_id}/workspaces",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

        if response.status_code == 200:
            workspaces = response.json().get("workspaces", [])
            for ws in workspaces:
                if ws["name"] == name:
                    self.delete_workspace(ws["id"])

    def modify_notebook_for_testing(
        self, notebook: nbformat.NotebookNode, notebook_path: Path
    ) -> nbformat.NotebookNode:
        """
        Modify notebook to use service app auth and test workspace.

        This function:
        1. Finds the first code cell containing authentication setup
        2. Extracts any imports and variable assignments from that cell
        3. Creates a new cell with service app auth + preserved imports
        4. Replaces the original auth cell with the new one
        """

        # Find the first code cell that contains authentication setup
        first_code_cell_idx = None
        original_imports = []

        for idx, cell in enumerate(notebook.cells):
            if cell.cell_type == "code":
                source = cell.source
                # Check if this is an auth cell
                if any(
                    keyword in source for keyword in ["client_id", "redirect_url", "ServiceManagerWidget", "manager"]
                ):
                    first_code_cell_idx = idx

                    # Extract imports from the original cell (handle multi-line imports)
                    lines = source.split("\n")
                    i = 0
                    while i < len(lines):
                        line = lines[i]
                        stripped = line.strip()

                        # Stop when we hit auth-related code
                        if any(
                            keyword in stripped
                            for keyword in ["client_id =", "redirect_url =", "ServiceManagerWidget", "manager ="]
                        ):
                            break

                        # Handle import statements (including multi-line)
                        if stripped.startswith("import ") or stripped.startswith("from "):
                            import_lines = [line]
                            # Check for multi-line imports (lines ending with backslash or unclosed parentheses)
                            paren_depth = line.count("(") - line.count(")")
                            while i + 1 < len(lines) and (line.rstrip().endswith("\\") or paren_depth > 0):
                                i += 1
                                line = lines[i]
                                import_lines.append(line)
                                paren_depth += line.count("(") - line.count(")")
                            original_imports.append("\n".join(import_lines))
                        # Handle variable assignments like cache_location, input_path
                        elif ("cache_location" in stripped and "=" in stripped) or (
                            "input_path" in stripped and "=" in stripped
                        ):
                            original_imports.append(line)

                        i += 1
                    break

        if first_code_cell_idx is None:
            print("⚠️ Warning: Could not find authentication cell to replace")
            return notebook

        # Build the new auth cell with preserved imports
        imports_section = "\n".join(original_imports) if original_imports else ""

        # Prepare hub_url with https:// prefix if needed
        hub_url_value = f"https://{self.hub_url}" if not self.hub_url.startswith("http") else self.hub_url

        auth_setup = f"""# CI Test Setup - Service App Authentication
import uuid
from evo.aio import AioTransport
from evo.oauth import ClientCredentialsAuthorizer, EvoScopes, OAuthConnector
from evo.common import APIConnector, Environment
from evo.common.utils.cache import Cache

{imports_section}

from evo.notebooks import FeedbackWidget
from evo.objects import ObjectAPIClient

cache_location = "data"
input_path = cache_location + "/input"
cache = Cache(root=cache_location, mkdir=True)

# Environment variables from CI
org_id = "{self.org_id}"
workspace_id = "{self.workspace_id}"
client_id = "{self.client_id}"
client_secret = "{self.client_secret}"
user_agent = "Evo CI Test/1.0"
hub_url = "{hub_url_value}"

# Create environment
environment = Environment(
    hub_url=hub_url, 
    org_id=uuid.UUID(org_id), 
    workspace_id=uuid.UUID(workspace_id)
)

# Create authorizer
authorizer = ClientCredentialsAuthorizer(
    oauth_connector=OAuthConnector(
        transport=AioTransport(user_agent=user_agent),
        client_id=client_id,
        client_secret=client_secret,
    ),
    scopes=EvoScopes.evo_discovery | EvoScopes.evo_workspace | EvoScopes.evo_object,
)

await authorizer.authorize()

# Create connector
connector = APIConnector(environment.hub_url, transport=AioTransport(user_agent=user_agent), authorizer=authorizer)

# For compatibility with notebooks that use auth_token directly
auth_token = (await authorizer.get_default_headers())["Authorization"].split("Bearer ")[1]
"""

        # Create the new auth cell
        auth_cell = nbformat.v4.new_code_cell(source=auth_setup)

        # Replace the first code cell with the auth cell
        if first_code_cell_idx is not None:
            notebook.cells[first_code_cell_idx] = auth_cell
        else:
            # Insert auth cell at the beginning (after first markdown cell if exists)
            insert_idx = 1 if notebook.cells and notebook.cells[0].cell_type == "markdown" else 0
            notebook.cells.insert(insert_idx, auth_cell)

        # Now find and fix any cells that reference manager.get_environment() or manager.get_connector()
        for cell in notebook.cells:
            if cell.cell_type == "code":
                if (
                    "manager.get_environment()" in cell.source
                    or "manager.get_connector()" in cell.source
                    or "manager.cache" in cell.source
                ):
                    # Replace manager references with direct variables
                    cell.source = cell.source.replace("manager.get_environment()", "environment")
                    cell.source = cell.source.replace("manager.get_connector()", "connector")
                    cell.source = cell.source.replace("manager.cache", "cache=cache")
                    # Also update ObjectAPIClient constructor to use named parameters
                    if "ObjectAPIClient(" in cell.source and "environment" in cell.source:
                        # Fix the constructor call to use named parameters
                        cell.source = cell.source.replace(
                            "ObjectAPIClient(environment, connector)",
                            "ObjectAPIClient(connector=connector, environment=environment)",
                        )

        return notebook

    def should_skip_notebook(self, notebook_path: Path) -> bool:
        """Determine if a notebook should be skipped."""
        skip_patterns = [
            "native-app-token",
            "service-app-token",
            "evo-discovery",
            "create-a-workspace",
            "delete-a-workspace",
        ]

        notebook_name = notebook_path.stem
        return any(pattern in notebook_name for pattern in skip_patterns)

    def run_notebook(self, notebook_path: Path) -> tuple[bool, str]:
        """Run a single notebook and return success status and message."""

        if self.should_skip_notebook(notebook_path):
            return True, "Skipped (auth/workspace management notebook)"

        print(f"\n{'=' * 80}")
        print(f"Testing: {notebook_path.relative_to(Path.cwd())}")
        print(f"{'=' * 80}")

        # Create debug directory at the very start, outside try-except
        debug_dir = Path("test-results/modified-notebooks")
        debug_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Debug directory created: {debug_dir.absolute()}")

        try:
            # Load the notebook
            print("📖 Loading notebook...")
            with open(notebook_path, "r", encoding="utf-8") as f:
                nb = nbformat.read(f, as_version=4)
            print("✓ Notebook loaded")

            # Create or use existing workspace
            workspace_to_delete = None
            if not self.workspace_id:
                # Try to create a test workspace
                workspace_name = f"test-{notebook_path.stem}-{int(time.time())}"
                try:
                    self.workspace_id = self.create_workspace(workspace_name)
                    workspace_to_delete = self.workspace_id  # Mark for cleanup
                except RuntimeError as e:
                    if "403" in str(e):
                        raise RuntimeError(
                            "Service app doesn't have permission to create workspaces. "
                            "Please set EVO_WORKSPACE_ID environment variable to use an existing workspace."
                        ) from e
                    raise
            else:
                print(f"📦 Using existing workspace: {self.workspace_id}")

            # Modify notebook for testing
            print("✏️ Modifying notebook...")
            nb = self.modify_notebook_for_testing(nb, notebook_path)
            print("✓ Notebook modified")

            # Save a copy for debugging IMMEDIATELY after modification
            debug_path = debug_dir / f"{notebook_path.stem}_modified.ipynb"
            print(f"💾 Saving modified notebook to: {debug_path.absolute()}")
            with open(debug_path, "w", encoding="utf-8") as f:
                nbformat.write(nb, f)
            print("✓ Modified notebook saved successfully")

            # Verify file was written
            if debug_path.exists():
                print(f"✓ Verified file exists: {debug_path.absolute()}")
            else:
                print("⚠️ Warning: File does not exist after write!")

            # Save modified notebook to temp file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as tmp:
                nbformat.write(nb, tmp)
                tmp_path = tmp.name

            try:
                # Execute the notebook
                client = NotebookClient(
                    nb, timeout=600, kernel_name="python3", resources={"metadata": {"path": str(notebook_path.parent)}}
                )
                client.execute()

                return True, "✓ Passed"

            except CellExecutionError as e:
                # Get more detailed error information
                error_msg = str(e)
                if hasattr(e, "traceback"):
                    error_msg += f"\n\nTraceback:\n{e.traceback}"
                return False, f"✗ Cell execution error: {error_msg[:2000]}"

            finally:
                # Cleanup temp file
                Path(tmp_path).unlink(missing_ok=True)
                # Delete workspace only if we created it during this test
                if workspace_to_delete:
                    self.delete_workspace(workspace_to_delete)

        except Exception as e:
            # Make sure to cleanup workspace if we created it
            if workspace_to_delete:
                self.delete_workspace(workspace_to_delete)
            return False, f"✗ Error: {str(e)[:200]}"

    def find_notebooks(self) -> List[Path]:
        """Find all Jupyter notebooks in the samples directory."""
        # Script is now in samples/.github/scripts, so samples dir is two levels up
        script_dir = Path(__file__).parent
        samples_dir = script_dir.parent.parent

        if not samples_dir.exists():
            raise FileNotFoundError(f"Samples directory not found: {samples_dir}")

        # TODO: TEMPORARY - Only test specific notebooks
        notebooks = [
            samples_dir / "geoscience-objects/publish-pointset/publish-pointset.ipynb",
            samples_dir / "geoscience-objects/publish-downhole-collection/publish-downhole-collection.ipynb",
        ]

        # Filter to only existing notebooks
        notebooks = [nb for nb in notebooks if nb.exists()]

        return sorted(notebooks)

    def run_all_tests(self) -> int:
        """Run tests on all notebooks and return exit code."""
        notebooks = self.find_notebooks()

        print(f"\nFound {len(notebooks)} notebooks to test")
        print("=" * 80)

        results = []

        for notebook_path in notebooks:
            success, message = self.run_notebook(notebook_path)
            results.append({"path": str(notebook_path.relative_to(Path.cwd())), "success": success, "message": message})

        # Print summary
        print(f"\n{'=' * 80}")
        print("TEST SUMMARY")
        print(f"{'=' * 80}")

        passed = sum(1 for r in results if r["success"])
        failed = len(results) - passed

        for result in results:
            status = "✓" if result["success"] else "✗"
            print(f"{status} {result['path']}: {result['message']}")

        print(f"\n{'=' * 80}")
        print(f"Total: {len(results)} | Passed: {passed} | Failed: {failed}")
        print(f"{'=' * 80}")

        # Save results
        results_dir = Path("test-results")
        results_dir.mkdir(exist_ok=True)

        with open(results_dir / "results.json", "w") as f:
            json.dump(results, f, indent=2)

        return 0 if failed == 0 else 1


def main():
    """Main entry point."""
    try:
        tester = NotebookTester()
        exit_code = tester.run_all_tests()
        sys.exit(exit_code)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
