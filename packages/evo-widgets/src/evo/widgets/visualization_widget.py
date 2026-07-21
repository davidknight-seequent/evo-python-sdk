"""The interactive Jupyter viewer for Evo visualisation-service tilesets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anywidget
import traitlets

from .visualization import TilesetBundle, VIRTUAL_ORIGIN

_ASSETS = Path(__file__).parent / "assets"


class EvoObjectViewer(anywidget.AnyWidget):
    """Render one or more downloaded Evo object tilesets in a notebook."""

    _esm = _ASSETS / "visualization.bundle.js"
    _css = _ASSETS / "visualization.css"

    _manifest = traitlets.Unicode("{}").tag(sync=True)
    _blob = traitlets.Bytes(b"").tag(sync=True)

    axis_labels = traitlets.List(traitlets.Unicode(), default_value=["X", "Y", "Z"]).tag(sync=True)
    show_axes = traitlets.Bool(True).tag(sync=True)
    tick_count = traitlets.Int(5).tag(sync=True)
    background_color = traitlets.Unicode("#121212").tag(sync=True)
    height = traitlets.Int(700).tag(sync=True)
    debug = traitlets.Bool(False).tag(sync=True)
    debug_max_lines = traitlets.Int(12).tag(sync=True)
    color_attribute = traitlets.Unicode("").tag(sync=True)
    colormap = traitlets.Unicode("viridis").tag(sync=True)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._bundles: list[TilesetBundle] = []

    def add_bundle(self, bundle: TilesetBundle) -> EvoObjectViewer:
        """Add a downloaded tileset bundle to the rendered scene."""
        self._bundles.append(bundle)
        self._repack()
        return self

    def clear(self) -> EvoObjectViewer:
        """Remove all objects from the rendered scene."""
        self._bundles.clear()
        self._repack()
        return self

    def _repack(self) -> None:
        """Pack tile files into one synchronized byte buffer and JSON manifest."""
        chunks: list[bytes] = []
        offset = 0
        objects: list[dict[str, Any]] = []
        for bundle in self._bundles:
            files: list[dict[str, Any]] = []
            root_path = f"{bundle.object_id}/tileset.json"
            root_bytes = json.dumps(bundle.tileset).encode("utf-8")
            for path, data in [(root_path, root_bytes), *bundle.files.items()]:
                chunks.append(data)
                files.append({"path": path, "offset": offset, "length": len(data)})
                offset += len(data)
            objects.append(
                {
                    "id": str(bundle.object_id),
                    "name": str(bundle.name),
                    "root": f"{VIRTUAL_ORIGIN}/{root_path}",
                    "files": files,
                    "attributes": list(bundle.attributes),
                }
            )
        self._blob = b"".join(chunks)
        self._manifest = json.dumps({"origin": VIRTUAL_ORIGIN, "objects": objects})