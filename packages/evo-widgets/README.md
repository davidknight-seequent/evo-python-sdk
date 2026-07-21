# evo-widgets

Widgets and presentation layer for the Evo Python SDK — HTML rendering, URL generation, and IPython formatters for Jupyter notebooks.

## Overview

This package provides rich HTML representations for Evo SDK objects in Jupyter notebooks. It decouples presentation logic from the data model classes, keeping the core SDK lightweight for production use while providing a batteries-included experience for notebook users.

## Installation

The package is included automatically when you install `evo-sdk`:

```bash
pip install evo-sdk
```

Or install it directly:

```bash
pip install evo-widgets
```

## Usage

### Enabling Rich Display in Notebooks

Load the IPython extension in your notebook to enable rich HTML rendering for all Evo SDK objects:

```python
%load_ext evo.widgets
```

After loading the extension, any Evo SDK typed object (e.g., `PointSet`, `Regular3DGrid`, `TensorGrid`) will automatically render with:

- Formatted metadata tables
- Clickable links to Portal and Viewer
- Bounding box information
- Coordinate reference system
- Attribute summaries

### Example

```python
from evo.notebooks import ServiceManagerWidget
from evo.objects.typed import object_from_path

# Authenticate
manager = await ServiceManagerWidget.with_auth_code(
    client_id="your-client-id"
).login()

# Load the widgets extension
%load_ext evo.widgets

# Load and display an object - it renders with rich HTML formatting
grid = await object_from_path(manager, "/My Grid")
grid  # Displays formatted HTML with Portal/Viewer links
```

## Features

### 3D Visualisation

Render supported Evo geoscience objects directly in a Jupyter notebook. The viewer downloads
OGC 3D Tiles through the authenticated SDK connector, so credentials never reach the browser.

```python
from evo.notebooks import ServiceManagerWidget
from evo.widgets import EvoObjectViewer, download_tileset_bundle, list_visualizable_objects

manager = await ServiceManagerWidget.with_auth_code(client_id="your-client-id").login()
objects = await list_visualizable_objects(manager)
bundle = await download_tileset_bundle(manager, objects[0].object_id, name=objects[0].name)

viewer = EvoObjectViewer(axis_labels=["Easting", "Northing", "Elevation"])
viewer.add_bundle(bundle)
viewer
```

Call `add_bundle()` again to layer objects in the same scene, or `clear()` to empty it. The
viewer supports visualisation-service pointsets, meshes, downhole data, and regular 2D grids.
Object attributes and their Evo colormaps are
included when available, allowing attribute-driven colouring in the viewer.

### HTML Formatters

Rich HTML representations for all typed geoscience objects:

- `PointSet`
- `Regular3DGrid`
- `TensorGrid`
- `Attributes`
- And all other typed objects inheriting from `_BaseObject`

### URL Generation

Generate Portal and Viewer URLs for objects:

```python
from evo.widgets import (
    get_portal_url_for_object,
    get_viewer_url_for_object,
    get_viewer_url_for_objects,
)

# Get Portal URL for a single object
portal_url = get_portal_url_for_object(grid)

# Get Viewer URL for a single object
viewer_url = get_viewer_url_for_object(grid)

# View multiple objects together in the Viewer
url = get_viewer_url_for_objects(manager, [grid, pointset, tensor_grid])
```

### Low-Level URL Utilities

For advanced use cases, low-level URL generation functions are also available:

```python
from evo.widgets import (
    get_evo_base_url,
    get_hub_code,
    get_portal_url,
    get_viewer_url,
    get_portal_url_from_reference,
    get_viewer_url_from_reference,
    serialize_object_reference,
)

# Generate URLs from components
portal_url = get_portal_url(
    org_id="org-123",
    workspace_id="ws-456",
    object_id="obj-789",
    hub_url="https://350mt.api.seequent.com",
)

# Generate URLs from object reference strings
ref_url = "https://350mt.api.seequent.com/geoscience-object/orgs/org-123/workspaces/ws-456/objects/obj-789"
portal_url = get_portal_url_from_reference(ref_url)
viewer_url = get_viewer_url_from_reference(ref_url)

```

## API Reference

### IPython Extension

| Function | Description |
|----------|-------------|
| `load_ipython_extension(ipython)` | Register HTML formatters for Evo SDK types |
| `unload_ipython_extension(ipython)` | Unregister HTML formatters |

### URL Functions (Object-Based)

| Function | Description |
|----------|-------------|
| `get_portal_url_for_object(obj)` | Generate Portal URL from a typed object |
| `get_viewer_url_for_object(obj)` | Generate Viewer URL from a typed object |
| `get_viewer_url_for_objects(context, objects)` | Generate Viewer URL for multiple objects |

### URL Functions (Low-Level)

| Function | Description |
|----------|-------------|
| `get_evo_base_url(hub_url)` | Get the Evo base URL from a hub URL |
| `get_hub_code(hub_url)` | Extract the hub code from a hub URL |
| `get_portal_url(org_id, workspace_id, object_id, hub_url)` | Generate Portal URL from components |
| `get_viewer_url(org_id, workspace_id, object_ids, hub_url)` | Generate Viewer URL from components |
| `get_portal_url_from_reference(object_reference)` | Generate Portal URL from reference string |
| `get_viewer_url_from_reference(object_reference)` | Generate Viewer URL from reference string |
| `serialize_object_reference(value)` | Serialize various object types to URL string |

### Formatters

| Function | Description |
|----------|-------------|
| `format_base_object(obj)` | Format a typed geoscience object as HTML |
| `format_attributes_collection(obj)` | Format an attributes collection as HTML |

### Visualisation

| API | Description |
|-----|-------------|
| `list_visualizable_objects(manager)` | List workspace objects supported by the 3D viewer |
| `download_tileset_bundle(manager, object_id)` | Download an authenticated 3D Tiles bundle for the browser |
| `EvoObjectViewer` | Display one or more downloaded bundles in a notebook |

## How It Works

When you run `%load_ext evo.widgets`, the extension registers HTML formatters with IPython using `for_type_by_name`. This approach:

1. **Avoids hard dependencies** — The widgets package doesn't import model classes directly
2. **Works with all typed objects** — Formatters are registered for the base class, so all subclasses are covered
3. **Lazy loading** — Formatters only activate when the relevant types are actually used

## CSS Customization

The HTML output uses CSS classes prefixed with `.evo` and respects Jupyter theme variables (e.g., `--jp-layout-color1`, `--jp-ui-font-color1`) for proper light/dark mode support.

## License

Apache License 2.0 — see [LICENSE.md](LICENSE.md) for details.

