"""Download Evo visualisation-service tilesets for notebook rendering.

The browser receives only downloaded tile bytes, never Evo credentials. The companion
:class:`evo.widgets.visualization_widget.EvoObjectViewer` renders the returned bundles.
"""

from __future__ import annotations

import asyncio
import gzip
import json
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, Iterator
from urllib.parse import urljoin, urlparse

from evo.common.data import HTTPResponse

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
VIRTUAL_ORIGIN = "https://evo.local"
_POLL_INTERVAL_SECONDS = 2.0
_MAX_POLL_ATTEMPTS = 60


@dataclass(frozen=True)
class VisualizableObject:
    """A geoscience object supported by the visualisation service."""

    object_id: str
    name: str
    schema: str
    version_id: str | None = None


@dataclass
class TilesetBundle:
    """A fully self-contained tileset ready for :class:`EvoObjectViewer`."""

    object_id: str
    name: str
    tileset: dict[str, Any]
    files: dict[str, bytes] = field(default_factory=dict)
    attributes: list[dict[str, Any]] = field(default_factory=list)
    collections: list[dict[str, Any]] = field(default_factory=list)

    @property
    def virtual_root(self) -> str:
        """Return the virtual URL used as the tileset entry point."""
        return f"{VIRTUAL_ORIGIN}/{self.object_id}/tileset.json"


def _connector_and_environment(manager: Any) -> tuple[Any, Any]:
    """Obtain the authenticated connector and selected environment from an SDK manager."""
    return manager.get_connector(), manager.get_environment()


def _schema_type(schema: str | None) -> str | None:
    if not schema:
        return None
    parts = [part for part in schema.split("/") if part]
    if "objects" in parts:
        index = parts.index("objects")
        if index + 1 < len(parts):
            return parts[index + 1].lower()
    last = parts[-1] if parts else ""
    return last.lower().replace(".schema.json", "") or None


def is_visualizable(schema: str | None) -> bool:
    """Return whether a schema is supported by the visualisation service."""
    return _schema_type(schema) in SUPPORTED_SCHEMAS


async def list_visualizable_objects(manager: Any, *, limit: int = 200) -> list[VisualizableObject]:
    """List supported objects in the manager's selected workspace."""
    connector, environment = _connector_and_environment(manager)
    response = await connector.call_api(
        method="GET",
        resource_path="/geoscience-object/orgs/{org_id}/workspaces/{workspace_id}/objects",
        path_params={"org_id": environment.org_id, "workspace_id": environment.workspace_id},
        query_params={"limit": str(limit)},
        header_params={"Accept": "application/json"},
        response_types_map={"200": HTTPResponse},
    )
    if response.status != HTTPStatus.OK:
        raise RuntimeError(f"Failed to list objects (HTTP {response.status}).")

    payload = json.loads(response.data.decode("utf-8"))
    raw_objects = payload.get("objects", payload if isinstance(payload, list) else [])
    return [
        VisualizableObject(
            object_id=obj.get("object_id") or obj.get("id"),
            name=obj.get("name", obj.get("object_id", "unnamed")),
            schema=obj["schema"],
            version_id=obj.get("version_id") or obj.get("versionId"),
        )
        for obj in raw_objects
        if is_visualizable(obj.get("schema"))
    ]


def _visualization_resource_path() -> str:
    return "/visualization/orgs/{org_id}/workspaces/{workspace_id}/geoscience-object/{object_id}"


async def fetch_tileset_json(manager: Any, object_id: str, *, version: str | None = None) -> dict[str, Any]:
    """Fetch an object's OGC 3D Tiles JSON, polling while it is generated."""
    connector, environment = _connector_and_environment(manager)
    query_params = {"version": version} if version else None
    for _ in range(_MAX_POLL_ATTEMPTS):
        response = await connector.call_api(
            method="GET",
            resource_path=_visualization_resource_path(),
            path_params={
                "org_id": environment.org_id,
                "workspace_id": environment.workspace_id,
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
        raise RuntimeError(f"Visualisation request failed for {object_id} (HTTP {response.status}).")
    raise TimeoutError(f"Tileset generation timed out for object {object_id}.")


def _iter_content_uris(tile: dict[str, Any]) -> Iterator[str]:
    content = tile.get("content")
    if isinstance(content, dict):
        uri = content.get("uri") or content.get("url")
        if uri:
            yield uri
    for entry in tile.get("contents", []) or []:
        if isinstance(entry, dict):
            uri = entry.get("uri") or entry.get("url")
            if uri:
                yield uri
    for child in tile.get("children", []) or []:
        yield from _iter_content_uris(child)


def _strip_gz_suffix(uri: str) -> str:
    query_index = uri.find("?")
    path = uri if query_index < 0 else uri[:query_index]
    suffix = "" if query_index < 0 else uri[query_index:]
    return path[:-3] + suffix if path.lower().endswith(".gz") else uri


def _normalize_tile_uris(tile: dict[str, Any]) -> None:
    for entry in _iter_content_entries(tile):
        for key in ("uri", "url"):
            if entry.get(key):
                entry[key] = _strip_gz_suffix(entry[key])
    for child in tile.get("children") or []:
        if isinstance(child, dict):
            _normalize_tile_uris(child)


def _iter_content_entries(tile: dict[str, Any]) -> Iterator[dict[str, Any]]:
    content = tile.get("content")
    if isinstance(content, dict):
        yield content
    for entry in tile.get("contents") or []:
        if isinstance(entry, dict):
            yield entry


def _as_tileset_ref(uri: str) -> str:
    """Ensure a nested-tileset URI ends with ``.json`` so the browser renderer treats it
    as an external tileset (3d-tiles-renderer selects the tileset parser by extension)."""
    query_index = uri.find("?")
    path = uri if query_index < 0 else uri[:query_index]
    suffix = "" if query_index < 0 else uri[query_index:]
    return uri if path.lower().endswith(".json") else f"{path}.json{suffix}"


def _mark_external_tileset_refs(tile: dict[str, Any], base_virtual_url: str, nested_paths: set[str]) -> None:
    """Rewrite references that point at downloaded nested tilesets so their URIs end in
    ``.json``. ``nested_paths`` holds the virtual paths that were identified as tilesets."""
    for entry in _iter_content_entries(tile):
        for key in ("uri", "url"):
            uri = entry.get(key)
            if not uri:
                continue
            path = _virtual_path_of(urljoin(base_virtual_url, uri))
            if path in nested_paths:
                entry[key] = _as_tileset_ref(uri)
    for child in tile.get("children") or []:
        if isinstance(child, dict):
            _mark_external_tileset_refs(child, base_virtual_url, nested_paths)


def _parse_nested_tileset(data: bytes) -> dict[str, Any] | None:
    """Return the parsed tileset if ``data`` is a nested tileset JSON, else ``None``.

    Detection is content-based rather than extension-based: some services (e.g. downhole
    collections) reference nested tilesets through extension-less URIs such as
    ``tileset_location``, so relying on a ``.json`` suffix misses them.
    """
    if data[:4] == b"glTF":
        return None
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if isinstance(parsed, dict) and isinstance(parsed.get("root"), dict):
        return parsed
    return None


def _maybe_gunzip(data: bytes) -> bytes:
    return gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data


def _hub_base(environment: Any) -> str:
    return environment.hub_url if environment.hub_url.endswith("/") else f"{environment.hub_url}/"


async def _download(connector: Any, hub_base: str, absolute_url: str) -> bytes:
    if absolute_url.startswith(hub_base):
        resource_path = "/" + absolute_url[len(hub_base) :].lstrip("/")
        response = await connector.call_api(
            method="GET", resource_path=resource_path, response_types_map={"200": HTTPResponse}
        )
        if response.status != HTTPStatus.OK:
            raise RuntimeError(f"Failed to download {resource_path} (HTTP {response.status}).")
        return response.data

    import httpx

    async with httpx.AsyncClient() as client:
        response = await client.get(absolute_url)
        response.raise_for_status()
        return response.content


def _virtual_path_of(virtual_url: str) -> str:
    return urlparse(virtual_url).path.lstrip("/")


def _collect_attribute_metadata(node: Any, attributes: list[dict[str, Any]], seen: set[str]) -> None:
    if isinstance(node, dict):
        values = node.get("values")
        data_hash = values.get("data") if isinstance(values, dict) else None
        if isinstance(node.get("name"), str) and isinstance(data_hash, str) and "key" in node and data_hash not in seen:
            seen.add(data_hash)
            attributes.append(
                {
                    "hash": data_hash,
                    "name": node["name"],
                    "key": str(node["key"]),
                    "kind": "category" if isinstance(node.get("table"), dict) else "continuous",
                    "attribute_type": node.get("attribute_type"),
                }
            )
        for value in node.values():
            _collect_attribute_metadata(value, attributes, seen)
    elif isinstance(node, list):
        for value in node:
            _collect_attribute_metadata(value, attributes, seen)


def _collect_data_hashes(node: Any, hashes: set[str]) -> None:
    if isinstance(node, dict):
        values = node.get("values")
        if isinstance(values, dict) and isinstance(values.get("data"), str):
            hashes.add(values["data"])
        for value in node.values():
            _collect_data_hashes(value, hashes)
    elif isinstance(node, list):
        for value in node:
            _collect_data_hashes(value, hashes)


def _collect_collections(object_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a per-collection view of a downhole object as ``[{name, hashes}]``.

    Non-downhole objects (those without a ``collections`` array) return ``[]`` so the viewer
    keeps its default single-object behaviour.
    """
    if not isinstance(object_json, dict) or "collections" not in object_json:
        return []
    defs: list[dict[str, Any]] = []
    location = object_json.get("location")
    if isinstance(location, dict):
        hashes: set[str] = set()
        _collect_data_hashes(location, hashes)
        defs.append({"name": "Collars", "hashes": hashes, "is_collar": True})
    for index, collection in enumerate(object_json.get("collections") or []):
        if not isinstance(collection, dict):
            continue
        hashes = set()
        _collect_data_hashes(collection, hashes)
        defs.append({"name": collection.get("name") or f"Collection {index + 1}", "hashes": hashes, "is_collar": False})
    return defs


def _assign_glbs_to_collections(bundle: TilesetBundle, collection_defs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map each downloaded glb to a collection by the attribute hashes embedded in it.

    Geometry without embedded attributes (e.g. collar points) falls back to the ``Collars``
    collection. Collections with no rendered geometry (e.g. depth tables) are dropped.
    """
    if not collection_defs:
        return []
    name_by_hash = {a["hash"]: a["name"] for a in bundle.attributes}
    fallback = next((c["name"] for c in collection_defs if c["name"] == "Collars"), collection_defs[0]["name"])
    assigned: dict[str, dict[str, Any]] = {c["name"]: {"glbs": [], "hashes": set()} for c in collection_defs}
    for path in [k for k in bundle.files if k.lower().endswith(".glb")]:
        data = bundle.files[path]
        present = {h for h in name_by_hash if h.encode("ascii") in data}
        best_name, best_score = None, 0
        for collection in collection_defs:
            score = len(present & collection["hashes"])
            if score > best_score:
                best_name, best_score = collection["name"], score
        entry = assigned[best_name or fallback]
        entry["glbs"].append(path)
        entry["hashes"].update(present)
    collections: list[dict[str, Any]] = []
    for collection in collection_defs:
        entry = assigned[collection["name"]]
        if not entry["glbs"]:
            continue
        collections.append(
            {
                "name": collection["name"],
                "glbs": entry["glbs"],
                "attributes": [name_by_hash[h] for h in entry["hashes"] if h in name_by_hash],
                "is_collar": bool(collection.get("is_collar")),
            }
        )
    return collections


async def _get_object_json(manager: Any, object_id: str, *, version: str | None = None) -> dict[str, Any] | None:
    """Fetch and return the geoscience object's JSON body, or ``None`` on failure."""
    connector, environment = _connector_and_environment(manager)
    try:
        response = await connector.call_api(
            method="GET",
            resource_path="/geoscience-object/orgs/{org_id}/workspaces/{workspace_id}/objects/{object_id}",
            path_params={
                "org_id": environment.org_id,
                "workspace_id": environment.workspace_id,
                "object_id": str(object_id),
            },
            query_params={"version": version} if version else None,
            header_params={"Accept": "application/json"},
            response_types_map={"200": HTTPResponse},
        )
        if response.status != HTTPStatus.OK:
            return None
        payload = json.loads(response.data.decode("utf-8"))
    except Exception:
        return None
    return payload.get("object", payload)


async def fetch_object_attributes(manager: Any, object_id: str, *, version: str | None = None) -> list[dict[str, Any]]:
    """Return display metadata for an object's available attributes."""
    object_json = await _get_object_json(manager, object_id, version=version)
    if object_json is None:
        return []
    attributes: list[dict[str, Any]] = []
    _collect_attribute_metadata(object_json, attributes, set())
    return attributes


def _normalise_rgb(color: Any) -> list[float] | None:
    if not isinstance(color, (list, tuple)) or len(color) < 3:
        return None
    try:
        return [max(0.0, min(1.0, float(color[index]) / 255.0)) for index in range(3)]
    except (TypeError, ValueError):
        return None


def _parse_colormap_detail(detail: dict[str, Any]) -> dict[str, Any] | None:
    colors = [color for color in (_normalise_rgb(color) for color in detail.get("colors", [])) if color]
    gradient = detail.get("gradient_controls") or []
    controls = detail.get("attribute_controls") or []
    category_map = detail.get("map")
    if category_map is not None and not gradient:
        count = min(len(category_map), len(colors))
        return {"kind": "category", "map": [str(item) for item in category_map[:count]], "colors": colors[:count]} if count else None
    if not colors or not controls:
        return None
    if len(gradient) != len(colors):
        gradient = [index / (len(colors) - 1) if len(colors) > 1 else 0.0 for index in range(len(colors))]
    try:
        return {
            "kind": "continuous",
            "min": float(controls[0]),
            "max": float(controls[-1]),
            "stops": [{"position": float(position), "color": color} for position, color in zip(gradient, colors)],
        }
    except (TypeError, ValueError):
        return None


async def fetch_object_colormaps(manager: Any, object_id: str) -> dict[str, dict[str, Any]]:
    """Return colormap definitions keyed by the associated attribute key."""
    connector, environment = _connector_and_environment(manager)
    base = "/colormap/orgs/{org_id}/workspaces/{workspace_id}"
    path_params = {"org_id": environment.org_id, "workspace_id": environment.workspace_id}
    try:
        response = await connector.call_api(
            method="GET",
            resource_path=f"{base}/objects/{{object_id}}/associations",
            path_params={**path_params, "object_id": str(object_id)},
            header_params={"Accept": "application/json"},
            response_types_map={"200": HTTPResponse},
        )
        if response.status != HTTPStatus.OK:
            return {}
        payload = json.loads(response.data.decode("utf-8"))
        associations = payload.get("associations") or payload.get("items") or []
    except Exception:
        return {}

    colormaps: dict[str, dict[str, Any]] = {}
    for association in associations:
        attribute_id = association.get("attribute_id") or association.get("attribute")
        colormap_id = association.get("colormap_id") or association.get("colormap")
        if not attribute_id or not colormap_id or str(attribute_id) in colormaps:
            continue
        try:
            response = await connector.call_api(
                method="GET",
                resource_path=f"{base}/colormaps/{{colormap_id}}",
                path_params={**path_params, "colormap_id": str(colormap_id)},
                header_params={"Accept": "application/json"},
                response_types_map={"200": HTTPResponse},
            )
            if response.status == HTTPStatus.OK:
                parsed = _parse_colormap_detail(json.loads(response.data.decode("utf-8")))
                if parsed:
                    colormaps[str(attribute_id)] = parsed
        except Exception:
            continue
    return colormaps


async def download_tileset_bundle(
    manager: Any, object_id: str, *, name: str | None = None, version: str | None = None
) -> TilesetBundle:
    """Download a tileset and all referenced content into a browser-safe bundle."""
    connector, environment = _connector_and_environment(manager)
    object_id = str(object_id)
    tileset = await fetch_tileset_json(manager, object_id, version=version)
    bundle = TilesetBundle(object_id=object_id, name=str(name or object_id), tileset=tileset)
    object_json = await _get_object_json(manager, object_id, version=version)
    attributes: list[dict[str, Any]] = []
    if object_json is not None:
        _collect_attribute_metadata(object_json, attributes, set())
    colormaps = await fetch_object_colormaps(manager, object_id)
    for attribute in attributes:
        if colormap := colormaps.get(str(attribute.get("key"))):
            attribute["colormap"] = colormap
    bundle.attributes.extend(attributes)

    hub_base = _hub_base(environment)
    real_root = urljoin(
        hub_base,
        _visualization_resource_path()
        .lstrip("/")
        .format(org_id=environment.org_id, workspace_id=environment.workspace_id, object_id=object_id),
    )
    if version:
        real_root = f"{real_root}?version={version}"
    queue = [(real_root, bundle.virtual_root, tileset)]
    seen: set[str] = set()
    # Nested tilesets are re-encoded after the whole tree is fetched: normalising their
    # URIs up front (e.g. stripping ``.gz``) would corrupt the URLs still needed to
    # download their content. ``nested_paths`` records which virtual paths are tilesets so
    # references to them can be rewritten to end in ``.json`` for the browser renderer.
    nested_tilesets: list[tuple[str, str, dict[str, Any]]] = []
    nested_paths: set[str] = set()
    while queue:
        real_tileset_url, virtual_tileset_url, current_tileset = queue.pop()
        for uri in _iter_content_uris(current_tileset.get("root", {})):
            real_child = urljoin(real_tileset_url, uri)
            virtual_child = urljoin(virtual_tileset_url, uri)
            key = _virtual_path_of(virtual_child)
            if key in seen:
                continue
            seen.add(key)
            data = _maybe_gunzip(await _download(connector, hub_base, real_child))
            normalized_key = _strip_gz_suffix(key)
            nested = _parse_nested_tileset(data)
            if nested is None:
                bundle.files[normalized_key] = data
                if normalized_key != key:
                    bundle.files[key] = data
                continue
            nested_paths.add(normalized_key)
            nested_tilesets.append((normalized_key, virtual_child, nested))
            queue.append((real_child, virtual_child, nested))

    # The root tileset is packed separately by the widget; normalise and flag its external
    # tileset references in place.
    if isinstance(tileset.get("root"), dict):
        _normalize_tile_uris(tileset["root"])
        _mark_external_tileset_refs(tileset["root"], bundle.virtual_root, nested_paths)

    # Store each nested tileset under a ``.json`` path so 3d-tiles-renderer recognises it as
    # an external tileset, normalising and flagging its own references first.
    for normalized_key, virtual_child, nested in nested_tilesets:
        if isinstance(nested.get("root"), dict):
            _normalize_tile_uris(nested["root"])
            _mark_external_tileset_refs(nested["root"], virtual_child, nested_paths)
        bundle.files[_as_tileset_ref(normalized_key)] = json.dumps(nested).encode("utf-8")

    # Group the downloaded geometry into selectable collections (downhole objects only).
    if object_json is not None:
        bundle.collections = _assign_glbs_to_collections(bundle, _collect_collections(object_json))
    return bundle