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
    if content and content.get("uri"):
        yield content["uri"]
    for entry in tile.get("contents", []) or []:
        if entry.get("uri"):
            yield entry["uri"]
    for child in tile.get("children", []) or []:
        yield from _iter_content_uris(child)


def _strip_gz_suffix(uri: str) -> str:
    query_index = uri.find("?")
    path = uri if query_index < 0 else uri[:query_index]
    suffix = "" if query_index < 0 else uri[query_index:]
    return path[:-3] + suffix if path.lower().endswith(".gz") else uri


def _normalize_tile_uris(tile: dict[str, Any]) -> None:
    content = tile.get("content")
    if isinstance(content, dict) and content.get("uri"):
        content["uri"] = _strip_gz_suffix(content["uri"])
    for entry in tile.get("contents") or []:
        if isinstance(entry, dict) and entry.get("uri"):
            entry["uri"] = _strip_gz_suffix(entry["uri"])
    for child in tile.get("children") or []:
        if isinstance(child, dict):
            _normalize_tile_uris(child)


def _is_json_uri(uri: str) -> bool:
    return uri.split("?", 1)[0].lower().endswith((".json", ".json.gz"))


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


async def fetch_object_attributes(manager: Any, object_id: str, *, version: str | None = None) -> list[dict[str, Any]]:
    """Return display metadata for an object's available attributes."""
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
            return []
        payload = json.loads(response.data.decode("utf-8"))
    except Exception:
        return []

    attributes: list[dict[str, Any]] = []
    _collect_attribute_metadata(payload.get("object", payload), attributes, set())
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
    attributes = await fetch_object_attributes(manager, object_id, version=version)
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
            bundle.files[normalized_key] = data
            if normalized_key != key:
                bundle.files[key] = data
            if _is_json_uri(uri):
                try:
                    nested = json.loads(data.decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    continue
                if isinstance(nested, dict) and isinstance(nested.get("root"), dict):
                    _normalize_tile_uris(nested["root"])
                    encoded = json.dumps(nested).encode("utf-8")
                    bundle.files[normalized_key] = encoded
                    if normalized_key != key:
                        bundle.files[key] = encoded
                queue.append((real_child, virtual_child, nested))
    if isinstance(tileset.get("root"), dict):
        _normalize_tile_uris(tileset["root"])
    return bundle