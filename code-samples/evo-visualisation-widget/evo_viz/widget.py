"""The ``EvoObjectViewer`` anywidget.

Packs one or more :class:`~evo_viz.visualization.TilesetBundle` objects into a single binary
blob plus a JSON manifest, and hands them to the front-end module (``static/widget.js``). The
front-end serves the bytes from an in-memory virtual origin and renders them with
``3d-tiles-renderer``, alongside 3D axes and numbered tick labels.

anywidget makes this cross-platform out of the box: the same widget renders in JupyterLab,
Notebook 7, VS Code and Google Colab.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import anywidget
import traitlets

from .visualization import TilesetBundle, VIRTUAL_ORIGIN

_STATIC = pathlib.Path(__file__).parent / "static"


class EvoObjectViewer(anywidget.AnyWidget):
    """A 3D viewer for Evo geoscience objects with axes and tick labels."""

    _esm = _STATIC / "widget_v3.js"
    _css = _STATIC / "widget.css"

    # --- data synced to the front-end -------------------------------------
    _manifest = traitlets.Unicode("{}").tag(sync=True)
    _blob = traitlets.Bytes(b"").tag(sync=True)

    # --- user-facing configuration ----------------------------------------
    axis_labels = traitlets.List(
        traitlets.Unicode(), default_value=["Easting", "Northing", "Elevation"]
    ).tag(sync=True)
    show_axes = traitlets.Bool(True).tag(sync=True)
    tick_count = traitlets.Int(5).tag(sync=True)
    background_color = traitlets.Unicode("#1e1e1e").tag(sync=True)
    height = traitlets.Int(600).tag(sync=True)
    debug = traitlets.Bool(False).tag(sync=True)
    debug_max_lines = traitlets.Int(12).tag(sync=True)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._bundles: list[TilesetBundle] = []

    # ----------------------------------------------------------------------
    def add_bundle(self, bundle: TilesetBundle) -> "EvoObjectViewer":
        """Add a downloaded tileset bundle to the scene and refresh the view."""
        self._bundles.append(bundle)
        self._repack()
        return self

    def clear(self) -> "EvoObjectViewer":
        """Remove all objects from the scene."""
        self._bundles.clear()
        self._repack()
        return self

    # ----------------------------------------------------------------------
    def _repack(self) -> None:
        """Serialise all bundles into ``_manifest`` (JSON) + ``_blob`` (one binary buffer)."""
        chunks: list[bytes] = []
        offset = 0
        manifest_objects: list[dict[str, Any]] = []

        for bundle in self._bundles:
            files_meta: list[dict[str, Any]] = []

            # The root tileset JSON is served as a virtual file too, so the front-end can
            # `fetch` it just like any other tile payload.
            root_path = f"{bundle.object_id}/tileset.json"
            root_bytes = json.dumps(bundle.tileset).encode("utf-8")
            chunks.append(root_bytes)
            files_meta.append(
                {"path": root_path, "offset": offset, "length": len(root_bytes)}
            )
            offset += len(root_bytes)

            for path, data in bundle.files.items():
                chunks.append(data)
                files_meta.append({"path": path, "offset": offset, "length": len(data)})
                offset += len(data)

            manifest_objects.append(
                {
                    "id": str(bundle.object_id),
                    "name": str(bundle.name),
                    "root": f"{VIRTUAL_ORIGIN}/{root_path}",
                    "files": files_meta,
                }
            )

        self._blob = b"".join(chunks)
        self._manifest = json.dumps(
            {"origin": VIRTUAL_ORIGIN, "objects": manifest_objects}
        )
