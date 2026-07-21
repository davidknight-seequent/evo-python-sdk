"""Evo Visualisation widget for Jupyter.

A small, cross-platform toolkit for rendering Evo geoscience objects (surfaces, pointsets,
downhole data, etc.) inside a notebook using the Evo Visualisation API, alongside 3D axes
and numbered tick labels.

Typical usage::

    from evo_viz import login, list_visualizable_objects, download_tileset_bundle
    from evo_viz import EvoObjectViewer

    manager = await login(client_id="...", redirect_url="...")

    objects = await list_visualizable_objects(manager)
    bundle = await download_tileset_bundle(manager, objects[0].object_id)

    viewer = EvoObjectViewer()
    viewer.add_bundle(bundle)
    viewer

Block models and Central objects are intentionally out of scope for this widget.
"""

from .auth import login, get_connector, get_environment
from .visualization import (
    VisualizableObject,
    list_visualizable_objects,
    fetch_tileset_json,
    download_tileset_bundle,
    SUPPORTED_SCHEMAS,
)
from .widget import EvoObjectViewer

__all__ = [
    "login",
    "get_connector",
    "get_environment",
    "VisualizableObject",
    "list_visualizable_objects",
    "fetch_tileset_json",
    "download_tileset_bundle",
    "SUPPORTED_SCHEMAS",
    "EvoObjectViewer",
]

__version__ = "0.1.0"
