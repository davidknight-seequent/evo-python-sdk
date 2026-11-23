# Copy geoscience objects between workspaces

This script archives the latest active geoscience objects from one workspace, then publishes missing objects to another.
It also exports and recreates block models, including their geometry, column data, units, groups, hidden groups and tags.
All source objects, including destination conflicts, are downloaded before any destination objects are created.

## Prerequisites

- Python 3.11.5 or later and this repository's SDK environment. From the repository root, run `uv sync` if it is not
  already set up. The script uses the SDK packages from this checkout, including PyArrow.
- A **native/public OAuth client ID for each environment**. The script opens a browser for each login and does not
  accept client secrets. Register the callback URI with each client; the default is
  `http://localhost:3000/signin-callback`.
- Both clients must permit `evo.discovery`, `evo.workspace`, `evo.object`, `evo.blocksync`, and `offline_access`.
  The source login needs permission to read the workspace; the destination login needs Editor or Owner access.
- The development IMS and Discovery base URLs. Ask your environment administrator if you do not know them; they are
  not assumed to be the production URLs. Both services must support the requested scopes and block-model preview
  features used for groups and tags.
- Enough local disk space for all object blobs and block-model query results, plus working space for uploads.

Do not paste tokens or secrets into the script or this chat. The local archive contains your data; store it somewhere
appropriate for that data and restrict access to it.

## Run

Create a `.env` file beside the script using the settings in [`.env.example`](.env.example). You can instead place it
in the directory where you run the script. If `.env` already exists, add the settings rather than overwriting it.
Fill in both client IDs, the development IMS and Discovery URLs, and the callback URI registered with each client.
You can also configure the source URLs and the output directory:

```dotenv
EVO_COPY_SOURCE_CLIENT_ID=your-production-client-id
EVO_COPY_SOURCE_REDIRECT_URI=http://localhost:3000/signin-callback
EVO_COPY_SOURCE_IMS_URL=https://ims.bentley.com
EVO_COPY_SOURCE_DISCOVERY_URL=https://discover.api.seequent.com

EVO_COPY_DESTINATION_CLIENT_ID=your-development-client-id
EVO_COPY_DESTINATION_REDIRECT_URI=http://localhost:3001/signin-callback
EVO_COPY_DESTINATION_IMS_URL=https://your-development-ims-host
EVO_COPY_DESTINATION_DISCOVERY_URL=https://your-development-discovery-host

EVO_COPY_OUTPUT_DIR=~/evo-workspace-copy
```

The script first checks for `.env` in the **current working directory**, then beside the script if no file is found.
If both exist, only the file in the current directory is read; their settings are not merged. `.env` is ignored by Git;
do not commit it or put client secrets or access tokens in it. The output directory is a parent folder that may already
exist; each confirmed run creates a new timestamped subfolder inside it, such as `run-20261123-151411`.

Run from the repository root:

```shell
uv run python code-samples/geoscience-objects/copy-workspace/copy_workspace.py
```

Command-line options remain available and override environment variables, which override `.env`. For example:

```shell
uv run python code-samples/geoscience-objects/copy-workspace/copy_workspace.py \
  --source-client-id "<production-client-id>" \
  --destination-client-id "<development-client-id>" \
  --destination-ims-url "https://<development-ims-host>" \
  --destination-discovery-url "https://<development-discovery-host>" \
  --output-dir "$HOME/evo-workspace-copy"
```

Source URLs default to `https://ims.bentley.com` and `https://discover.api.seequent.com`, and callback URIs default
to `http://localhost:3000/signin-callback` if not set. Run with `--help` to see all command-line options and their
corresponding `.env` names.

Sign in and select the source instance, hub and workspace, then repeat for the destination. Check the displayed
environment details carefully and type `COPY` to continue. Any other response cancels without copying. The script
rejects the same workspace or hub as both source and destination. This guard cannot identify every production hub, so
you must still verify that the destination is in the development environment.

Try a small, non-critical workspace first and verify the resulting objects in the destination before using this for a
larger migration. Avoid modifying either workspace during a run: listing is not a workspace-wide snapshot, although
each listed object and its block-model data are fetched by version where available.

## Archive and conflicts

Each run's subfolder contains:

- `objects/`: original object JSON, named by source object ID.
- `blobs/`: referenced object data, named by its source identifier. SHA-256-named blobs are checked after download.
- `block-models/`: source model and version metadata, and Parquet data for models with columns or sub-block geometry.
- `manifest.jsonl`: source and destination environments, archive entries, publishing progress and outcomes. Blob
  entries include their local SHA-256 digests.
- `.cache/`: SDK working files; it may contain partial downloads if a run is interrupted.

The archive does not contain OAuth tokens or pre-signed URLs. For a block-model reference without an explicit model
version, the script reports that it uses the latest available block-model version.

All objects and block models are published to the **root folder** of the destination workspace; source folders are
discarded, so `geology/drillholes/collars.json` becomes `collars.json`. The manifest records both the source `path`
and the `destination_path`.

An existing destination object path is skipped, never updated. Because folders are flattened, objects with the same
filename in different source folders collide: the first one processed is published and later ones are skipped. Block
models are also skipped when their name already exists anywhere in the destination workspace. If an object appears
after the initial conflict check, a creation conflict is skipped. Existing content is not checked for equality.

The script stops on other errors. If archiving fails, publishing has not started. If publishing a block model fails
after creation, the destination may contain an **incomplete model**; the script reports this and does not delete it.
Inspect the destination and manifest before retrying. A new run archives into a new subfolder and will skip that
existing model unless you have dealt with it separately. The script does not resume from a previous archive, and old
run folders are not deleted automatically.

## Scope and limitations

- Only current active object versions are copied. History, stages, authorship, timestamps, permissions and workspace
  settings are not reproduced. New object, block-model, column and version IDs are assigned by the destination.
  System-generated sub-block derivation provenance is not retained, even though the resulting values are copied.
- EPSG coordinate reference systems are stripped from published copies: any object `coordinate_reference_system`
  of the form `{"epsg_code": ...}` (at any depth) is replaced with `"unspecified"`, and block models with an `EPSG:`
  CRS are created without one. WKT definitions are kept. The local archive retains the original values.
- The script does not migrate files from the File API, block-model reports, colormaps, or other separate services.
  References in object content to other objects or external resources are not rewritten and may still point to the
  source environment.
- Block-model reference objects are recreated by the Block Model Service rather than posted directly. Columnless
  sub-blocked models use a temporary column to transfer geometry, then delete it. If the service rejects the
  geometry-only query or transfer, the run stops rather than silently omitting geometry.
- The source workspace's objects and models are not modified; reading block-model data starts query jobs in the source
  service. Publishing creates objects and block models only in the selected destination.
