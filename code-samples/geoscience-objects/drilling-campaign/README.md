# Drilling Campaign Examples

This directory contains five Jupyter notebooks demonstrating how to work with **Evo Drilling Campaign** geoscience objects using the high-level Python SDK. These examples show how to create drilling campaign objects from CSV data, how to publish only a planned section, how to download existing drilling campaign objects, how to publish a new interim section onto an existing drilling campaign, and how to publish progressive interim snapshots over multiple runs.

## Notebooks

| Notebook | Purpose | Location |
|----------|---------|----------|
| `create-a-drilling-campaign/sdk-examples.ipynb` | Create drilling campaign objects from CSV data | Create and publish collar data, planned data, and interim data |
| `create-a-drilling-campaign-planned-only/sdk-examples.ipynb` | Create a drilling campaign with only the planned section | Publish a new drilling campaign object and stop after the initial planned-section publish |
| `download-a-drilling-campaign/sdk-examples.ipynb` | Download drilling campaign objects to CSV format | List, retrieve, and export drilling campaign data from Evo |
| `update-a-drilling-campaign/sdk-examples.ipynb` | Update an existing drilling campaign with interim data | Select an existing drilling campaign, copy its planned section, add interim data, and publish a new version |
| `update-a-drilling-campaign/progressive-sdk-examples.ipynb` | Incrementally publish interim drilling campaign data | Publish one interim snapshot per run and track the next snapshot index in `notebook-data/.env` |

## What You Can Do

### Creating a Drilling Campaign
1. Authenticate with Evo.
2. Load drilling campaign data from CSV files (collar, survey, attributes).
3. Create hole ID mappings and define object attributes.
4. Build drilling campaign components (collar, planned, interim data).
5. Assemble and publish the complete drilling campaign object to Evo.

### Creating a Planned-Only Drilling Campaign
1. Authenticate with Evo.
2. Load drilling campaign data from CSV files.
3. Create the collar and planned components.
4. Assemble the drilling campaign object.
5. Publish the object after the initial planned-section upload.

### Downloading a Drilling Campaign
1. Authenticate with Evo.
2. List all drilling campaign objects in a workspace.
3. Select and retrieve a specific drilling campaign object.
4. Download associated data (collar, planned, interim).
5. Export data to CSV format for further analysis.

### Updating a Drilling Campaign
1. Authenticate with Evo.
2. List all drilling campaign objects in a workspace.
3. Select and retrieve an existing drilling campaign object.
4. Copy the existing planned section from the downloaded object.
5. Build a new interim section from CSV data and publish a new object version.

### Incremental Interim Publishing
1. Authenticate with Evo.
2. Select an existing drilling campaign object.
3. Read the next progressive interim snapshot from `sample-data/interim-progressive`.
4. Publish that single snapshot as the current interim state.
5. Advance the counter stored in `notebook-data/.env` so the next run publishes the next snapshot.

## Prerequisites

- A Seequent account with Evo entitlements.
- An Evo application (client ID + redirect URL).
- The Python package manager `uv` installed.

Take a look at `code-samples/README.md` for more detailed information.

## Quick Start

From the `code-samples` directory:

```bash
# 1. Create a Python environment and install dependencies with uv
uv sync

# 2. Launch Jupyter (choose your notebook UI)
uv run jupyter lab  # or: uv run jupyter notebook

# 3. Open one of the notebooks
open geoscience-objects/drilling-campaign/create-a-drilling-campaign/sdk-examples.ipynb
open geoscience-objects/drilling-campaign/create-a-drilling-campaign-planned-only/sdk-examples.ipynb
open geoscience-objects/drilling-campaign/download-a-drilling-campaign/sdk-examples.ipynb
open geoscience-objects/drilling-campaign/update-a-drilling-campaign/sdk-examples.ipynb
open geoscience-objects/drilling-campaign/update-a-drilling-campaign/progressive-sdk-examples.ipynb
```

If using VS Code, you can simply open the notebooks directly; the Python / ipykernel environment should point at the synced virtual environment.

## Sample Data

The **create** notebook includes sample CSV data in its `sample-data/` directory with example collar, survey, and attribute files. The **create planned-only** notebook includes a `planned.csv` sample used to publish only the planned section of a new object. The **update** notebook includes an `interim.csv` sample used to build a new interim section for an existing object. The **incremental update** notebook uses the `interim-progressive/` snapshot set and stores the next snapshot index in `notebook-data/.env`. The **download** notebook works with existing drilling campaign objects in your Evo workspace and does not require sample data.

## Additional Resources

- [Seequent Developer Portal](https://developer.seequent.com)
- [Apps and tokens guide](https://developer.seequent.com/docs/guides/getting-started/apps-and-tokens)
- [Drilling Campaign schema documentation](https://developer.seequent.com/docs/data-structures/geoscience-objects/schemas/drilling-campaign)
