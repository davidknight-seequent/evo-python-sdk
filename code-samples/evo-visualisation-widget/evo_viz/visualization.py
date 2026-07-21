"""Evo Visualisation API access: list objects, fetch tilesets, download tile content.

This mirrors what the native EvoViewer app does, but in Python:

  * List geoscience objects in the selected workspace.
  * Keep only the schema types the Visualisation service can turn into 3D Tiles, and
    deliberately drop ``block-model`` (out of scope for this widget). Central objects never
    appear here because they come from a different service entirely.
  * Ask the Visualisation service for an OGC 3D Tiles tileset, polling on HTTP 202 while the
    tileset is generated (exactly like ``EvoAPIClient.fetchTileset`` in the app).
  * Walk the tile tree and download every referenced glTF/glb/JSON payload through the
    authenticated connector, so the browser never needs an Evo token (no CORS headaches).

The download step returns a *bundle*: the tileset JSON plus a flat ``{virtual_path: bytes}``
map. The front-end serves those bytes from an in-memory virtual origin, so
``3d-tiles-renderer`` can stream them as if they were normal files.
"""

from __future__ import annotations

import asyncio
import gzip
import posixpath
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, Iterable, Iterator
from urllib.parse import urljoin, urlparse

try:
    from evo.common.data import HTTPResponse
except ImportError as exc:  # pragma: no cover - environment specific
    raise ImportError(
        "The Evo Python SDK is required. Install it with `pip install evo-sdk-common`."
    ) from exc

from .auth import get_connector, get_environment

# ---------------------------------------------------------------------------
# Schema support
# ---------------------------------------------------------------------------
#
# These are the object types the Visualisation service can render, taken from the app's
# `supported_versions.json`. `block-model` is intentionally excluded from this widget.
SUPPORTED_SCHEMAS: frozenset[str] = frozenset(
    {
        "downhole-intervals",
        "downhole-collection",
        "triangle-mesh",
        "geological-model-meshes",
        "planar-data-pointset",
        "pointset",
        "regular-2d-grid",
    }
)

# The virtual host the front-end uses to serve downloaded bytes. Must match widget.js.
VIRTUAL_ORIGIN = "https://evo.local"

_POLL_INTERVAL_SECONDS = 2.0
_MAX_POLL_ATTEMPTS = 60


@dataclass(frozen=True)
class VisualizableObject:
    """A geoscience object that this widget can render."""

    object_id: str
    name: str
    schema: str
    version_id: str | None = None


def _schema_type(schema: str | None) -> str | None:
    """Extract the object type from a schema URI like
    ``/objects/pointset/1.2.0/pointset.schema.json`` -> ``pointset``.
    """
    if not schema:
        return None
    parts = [p for p in schema.split("/") if p]
    if "objects" in parts:
        idx = parts.index("objects")
        if idx + 1 < len(parts):
            return parts[idx + 1].lower()
    # Fallback: derive from the trailing "<type>.schema.json".
    last = parts[-1] if parts else ""
    return last.lower().replace(".schema.json", "") or None


def is_visualizable(schema: str | None) -> bool:
    """True when the schema is a supported, non-block-model type."""
    return _schema_type(schema) in SUPPORTED_SCHEMAS


# ---------------------------------------------------------------------------
# Listing objects
# ---------------------------------------------------------------------------
async def list_visualizable_objects(
    manager: Any,
    *,
    limit: int = 200,
) -> list[VisualizableObject]:
    """List objects in the selected workspace that this widget can render.

    Block models and unsupported schema types are filtered out.
    """
    connector = get_connector(manager)
    env = get_environment(manager)

    resource_path = "/geoscience-object/orgs/{org_id}/workspaces/{workspace_id}/objects"
    response = await connector.call_api(
        method="GET",
        resource_path=resource_path,
        path_params={"org_id": env.org_id, "workspace_id": env.workspace_id},
        query_params={"limit": str(limit)},
        header_params={"Accept": "application/json"},
        response_types_map={"200": HTTPResponse},
    )
    if response.status != HTTPStatus.OK:
        raise RuntimeError(f"Failed to list objects (HTTP {response.status}).")

    import json

    payload = json.loads(response.data.decode("utf-8"))
    raw_objects = payload.get("objects", payload if isinstance(payload, list) else [])

    results: list[VisualizableObject] = []
    for obj in raw_objects:
        schema = obj.get("schema")
        if not is_visualizable(schema):
            continue
        results.append(
            VisualizableObject(
                object_id=obj.get("object_id") or obj.get("id"),
                name=obj.get("name", obj.get("object_id", "unnamed")),
                schema=schema,
                version_id=obj.get("version_id") or obj.get("versionId"),
            )
        )
    return results


# ---------------------------------------------------------------------------
# Tileset fetch (with 202 polling)
# ---------------------------------------------------------------------------
def _visualization_resource_path() -> str:
    return (
        "/visualization/orgs/{org_id}/workspaces/{workspace_id}"
        "/geoscience-object/{object_id}"
    )


async def fetch_tileset_json(
    manager: Any,
    object_id: str,
    *,
    version: str | None = None,
) -> dict[str, Any]:
    """Fetch the OGC 3D Tiles tileset JSON for an object, polling on HTTP 202.

    The Visualisation service generates tilesets on demand; the first request often returns
    202 while generation is in progress. We poll until it returns 200, mirroring the app.
    """
    import json

    connector = get_connector(manager)
    env = get_environment(manager)

    query_params = {"version": version} if version else None

    for attempt in range(_MAX_POLL_ATTEMPTS):
        response = await connector.call_api(
            method="GET",
            resource_path=_visualization_resource_path(),
            path_params={
                "org_id": env.org_id,
                "workspace_id": env.workspace_id,
                "object_id": object_id,
            },
            query_params=query_params,
            header_params={"Accept": "application/json"},
            response_types_map={"200": HTTPResponse, "202": HTTPResponse},
        )

        if response.status == HTTPStatus.OK:
            return json.loads(response.data.decode("utf-8"))
        if response.status == HTTPStatus.ACCEPTED:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
            continue
        raise RuntimeError(
            f"Visualisation request failed for {object_id} (HTTP {response.status})."
        )

    raise TimeoutError(f"Tileset generation timed out for object {object_id}.")


# ---------------------------------------------------------------------------
# Tile-tree traversal + content download
# ---------------------------------------------------------------------------
def _iter_content_uris(tile: dict[str, Any]) -> Iterator[str]:
    """Yield every content URI referenced by a tile and its descendants."""
    content = tile.get("content")
    if content and content.get("uri"):
        yield content["uri"]
    for entry in tile.get("contents", []) or []:
        if entry.get("uri"):
            yield entry["uri"]
    for child in tile.get("children", []) or []:
        yield from _iter_content_uris(child)


def _strip_gz_suffix(uri: str) -> str:
    lower = uri.lower()
    if lower.endswith(".gz"):
        return uri[:-3]
    q = uri.find("?")
    if q > 0 and uri[:q].lower().endswith(".gz"):
        return uri[: q - 3] + uri[q:]
    return uri


def _normalize_tile_uris(tile: dict[str, Any]) -> None:
    content = tile.get("content")
    if isinstance(content, dict) and content.get("uri"):
        content["uri"] = _strip_gz_suffix(content["uri"])

    contents = tile.get("contents") or []
    for entry in contents:
        if isinstance(entry, dict) and entry.get("uri"):
            entry["uri"] = _strip_gz_suffix(entry["uri"])

    for child in tile.get("children", []) or []:
        if isinstance(child, dict):
            _normalize_tile_uris(child)


def _is_json_uri(uri: str) -> bool:
    base = uri.split("?", 1)[0].lower()
    return base.endswith(".json") or base.endswith(".json.gz")


def _maybe_gunzip(data: bytes) -> bytes:
    # Gzip magic bytes 0x1f, 0x8b.
    if len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B:
        return gzip.decompress(data)
    return data


@dataclass
class TilesetBundle:
    """A fully self-contained tileset ready to hand to the widget."""

    object_id: str
    name: str
    tileset: dict[str, Any]
    files: dict[str, bytes] = field(default_factory=dict)
    attributes: list[dict[str, Any]] = field(default_factory=list)

    @property
    def virtual_root(self) -> str:
        """The virtual URL the front-end should load as the tileset entry point."""
        return f"{VIRTUAL_ORIGIN}/{self.object_id}/tileset.json"


def _hub_base(env: Any) -> str:
    base = env.hub_url
    return base if base.endswith("/") else base + "/"


async def _download(connector: Any, hub_base: str, absolute_url: str) -> bytes:
    """Download bytes for a resolved absolute URL.

    Hub-hosted content goes through the authenticated connector. Anything else (e.g. a
    pre-signed blob URL) is fetched without an Evo token via httpx, if available.
    """
    if absolute_url.startswith(hub_base) or absolute_url.startswith(hub_base.rstrip("/")):
        resource_path = "/" + absolute_url[len(hub_base):].lstrip("/")
        response = await connector.call_api(
            method="GET",
            resource_path=resource_path,
            response_types_map={"200": HTTPResponse},
        )
        if response.status != HTTPStatus.OK:
            raise RuntimeError(f"Failed to download {resource_path} (HTTP {response.status}).")
        return response.data

    # External / pre-signed URL — no Evo auth header required.
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            f"Content is hosted off-hub ({absolute_url}); install httpx to download it."
        ) from exc
    async with httpx.AsyncClient() as client:
        resp = await client.get(absolute_url)
        resp.raise_for_status()
        return resp.content


def _virtual_path_of(virtual_url: str) -> str:
    """Return the path (without origin) used as the file-map key."""
    parsed = urlparse(virtual_url)
    return parsed.path.lstrip("/")


# ---------------------------------------------------------------------------
# Attribute metadata (friendly names for the glb property tables)
# ---------------------------------------------------------------------------
#
# The Visualisation service stores per-vertex attribute values inside each glb's
# ``EXT_structural_metadata`` property tables, but keyed by an opaque property id of the
# form ``attribute_<hash>``. That ``<hash>`` is the SHA-256 content hash of the attribute's
# stored value array — i.e. the ``values.data`` field on the geoscience object attribute.
# We fetch the object JSON, collect every attribute's ``{hash, name}`` here, and ship that
# small mapping to the front-end so the colour dropdown can show real names (CU_pct, etc.).
def _collect_attribute_metadata(
    node: Any, out: list[dict[str, Any]], seen: set[str]
) -> None:
    """Recursively find attribute-like dicts and record their ``{hash, name, kind}``."""
    if isinstance(node, dict):
        name = node.get("name")
        values = node.get("values")
        data_hash = values.get("data") if isinstance(values, dict) else None
        if isinstance(name, str) and isinstance(data_hash, str) and "key" in node:
            if data_hash not in seen:
                seen.add(data_hash)
                out.append(
                    {
                        "hash": data_hash,
                        "name": name,
                        "key": str(node.get("key")),
                        "kind": "category" if isinstance(node.get("table"), dict) else "continuous",
                        "attribute_type": node.get("attribute_type"),
                    }
                )
        for value in node.values():
            _collect_attribute_metadata(value, out, seen)
    elif isinstance(node, list):
        for value in node:
            _collect_attribute_metadata(value, out, seen)


async def fetch_object_attributes(
    manager: Any,
    object_id: str,
    *,
    version: str | None = None,
) -> list[dict[str, Any]]:
    """Return ``[{hash, name, kind, attribute_type}, ...]`` for an object's attributes.

    Best-effort: returns an empty list if the object cannot be fetched or has no attributes,
    so visualisation still works without attribute-driven colouring.
    """
    import json

    connector = get_connector(manager)
    env = get_environment(manager)

    query_params = {"version": version} if version else None
    try:
        response = await connector.call_api(
            method="GET",
            resource_path="/geoscience-object/orgs/{org_id}/workspaces/{workspace_id}/objects/{object_id}",
            path_params={
                "org_id": env.org_id,
                "workspace_id": env.workspace_id,
                "object_id": str(object_id),
            },
            query_params=query_params,
            header_params={"Accept": "application/json"},
            response_types_map={"200": HTTPResponse},
        )
    except Exception:  # pragma: no cover - network/permission failures are non-fatal
        return []

    if response.status != HTTPStatus.OK:
        return []

    try:
        payload = json.loads(response.data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return []

    attributes: list[dict[str, Any]] = []
    _collect_attribute_metadata(payload.get("object", payload), attributes, set())
    return attributes


def _normalise_rgb(color: Any) -> list[float] | None:
    """Convert an RGB(A) colour (0-255 ints) to normalized [r, g, b] floats in 0..1."""
    if not isinstance(color, (list, tuple)) or len(color) < 3:
        return None
    try:
        return [max(0.0, min(1.0, float(color[i]) / 255.0)) for i in range(3)]
    except (TypeError, ValueError):
        return None


def _parse_colormap_detail(detail: dict[str, Any]) -> dict[str, Any] | None:
    """Map a colormap-service payload onto the shape the renderer consumes.

    Continuous -> {kind, min, max, stops:[{position, color}]}; the value range comes from
    ``attribute_controls`` and the gradient from ``gradient_controls`` + ``colors``.
    Category   -> {kind, map:[labels], colors:[[r,g,b]]}.
    """
    colors = [c for c in (_normalise_rgb(c) for c in detail.get("colors", []) or []) if c]

    gradient = detail.get("gradient_controls") or []
    attribute_controls = detail.get("attribute_controls") or []
    category_map = detail.get("map")

    # Category colormap: a label list paired with per-category colours.
    if category_map is not None and not gradient:
        labels = [str(m) for m in category_map]
        n = min(len(labels), len(colors))
        if not n:
            return None
        return {"kind": "category", "map": labels[:n], "colors": colors[:n]}

    # Continuous colormap.
    if colors and attribute_controls:
        if len(gradient) != len(colors):
            # Synthesize evenly spaced positions if the service omitted them.
            n = len(colors)
            gradient = [i / (n - 1) if n > 1 else 0.0 for i in range(n)]
        stops = [
            {"position": float(pos), "color": col}
            for pos, col in zip(gradient, colors)
        ]
        try:
            vmin = float(attribute_controls[0])
            vmax = float(attribute_controls[-1])
        except (TypeError, ValueError):
            return None
        return {"kind": "continuous", "min": vmin, "max": vmax, "stops": stops}

    return None


async def fetch_object_colormaps(
    manager: Any,
    object_id: str,
) -> dict[str, dict[str, Any]]:
    """Return ``{attribute_key: colormap_dict}`` for an object's associated colormaps.

    Colour definitions live in Evo's colormap service (not in the tiles), so they are fetched
    here through the authenticated connector. Best-effort: any failure yields ``{}`` so
    visualisation still works, falling back to a default gradient in the browser.
    """
    import json

    connector = get_connector(manager)
    env = get_environment(manager)
    base = "/colormap/orgs/{org_id}/workspaces/{workspace_id}"
    path_params = {"org_id": env.org_id, "workspace_id": env.workspace_id}

    try:
        response = await connector.call_api(
            method="GET",
            resource_path=base + "/objects/{object_id}/associations",
            path_params={**path_params, "object_id": str(object_id)},
            header_params={"Accept": "application/json"},
            response_types_map={"200": HTTPResponse},
        )
    except Exception:  # pragma: no cover - non-fatal
        return {}
    if response.status != HTTPStatus.OK:
        return {}

    try:
        payload = json.loads(response.data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {}

    associations = payload.get("associations") or payload.get("items") or []
    result: dict[str, dict[str, Any]] = {}
    for assoc in associations:
        attribute_id = assoc.get("attribute_id") or assoc.get("attribute")
        colormap_id = assoc.get("colormap_id") or assoc.get("colormap")
        if not attribute_id or not colormap_id or str(attribute_id) in result:
            continue
        try:
            detail_resp = await connector.call_api(
                method="GET",
                resource_path=base + "/colormaps/{colormap_id}",
                path_params={**path_params, "colormap_id": str(colormap_id)},
                header_params={"Accept": "application/json"},
                response_types_map={"200": HTTPResponse},
            )
        except Exception:  # pragma: no cover - non-fatal
            continue
        if detail_resp.status != HTTPStatus.OK:
            continue
        try:
            detail = json.loads(detail_resp.data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            continue
        parsed = _parse_colormap_detail(detail)
        if parsed:
            result[str(attribute_id)] = parsed

    return result


async def download_tileset_bundle(
    manager: Any,
    object_id: str,
    *,
    name: str | None = None,
    version: str | None = None,
) -> TilesetBundle:
    """Download a tileset and all of its content into a self-contained bundle.

    The bundle can be handed straight to :class:`~evo_viz.widget.EvoObjectViewer`. Content is
    downloaded through the authenticated connector so the browser never needs an Evo token.
    """
    connector = get_connector(manager)
    env = get_environment(manager)
    hub_base = _hub_base(env)

    object_id = str(object_id)
    object_name = str(name) if name is not None else object_id

    tileset = await fetch_tileset_json(manager, object_id, version=version)

    # Real URL of the root tileset (used to resolve + download content from the hub).
    real_root = urljoin(
        hub_base,
        _visualization_resource_path()
        .lstrip("/")
        .format(
            org_id=env.org_id,
            workspace_id=env.workspace_id,
            object_id=object_id,
        ),
    )
    if version:
        real_root = f"{real_root}?version={version}"

    virtual_root = f"{VIRTUAL_ORIGIN}/{object_id}/tileset.json"

    bundle = TilesetBundle(object_id=object_id, name=object_name, tileset=tileset)

    # Friendly attribute names for the glb property tables (used by the colour dropdown), each
    # paired with its Evo colormap (fetched from the colormap service, keyed by attribute key).
    object_attributes = await fetch_object_attributes(manager, object_id, version=version)
    colormaps = await fetch_object_colormaps(manager, object_id)
    for attr in object_attributes:
        colormap = colormaps.get(str(attr.get("key")))
        if colormap:
            attr["colormap"] = colormap
    bundle.attributes.extend(object_attributes)

    # Breadth-first walk over (real_tileset_url, virtual_tileset_url, tileset_dict).
    queue: list[tuple[str, str, dict[str, Any]]] = [(real_root, virtual_root, tileset)]
    seen: set[str] = set()

    while queue:
        real_ts_url, virtual_ts_url, ts = queue.pop()
        for uri in _iter_content_uris(ts.get("root", {})):
            real_child = urljoin(real_ts_url, uri)
            virtual_child = urljoin(virtual_ts_url, uri)
            key = _virtual_path_of(virtual_child)
            if key in seen:
                continue
            seen.add(key)

            raw_data = await _download(connector, hub_base, real_child)
            data = _maybe_gunzip(raw_data)

            key_no_gz = _strip_gz_suffix(key)
            bundle.files[key_no_gz] = data
            if key_no_gz != key:
                # Keep the original key too as a compatibility alias.
                bundle.files[key] = data

            # A nested tileset (external .json) — recurse into its content.
            if _is_json_uri(uri):
                import json

                try:
                    nested = json.loads(data.decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    continue

                if isinstance(nested, dict) and isinstance(nested.get("root"), dict):
                    _normalize_tile_uris(nested["root"])
                    encoded = json.dumps(nested).encode("utf-8")
                    bundle.files[key_no_gz] = encoded
                    if key_no_gz != key:
                        bundle.files[key] = encoded

                queue.append((real_child, virtual_child, nested))

    if isinstance(bundle.tileset, dict) and isinstance(bundle.tileset.get("root"), dict):
        _normalize_tile_uris(bundle.tileset["root"])

    return bundle
