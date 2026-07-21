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
