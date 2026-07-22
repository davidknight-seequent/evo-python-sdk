"""Tests for Evo visualisation backend helpers."""

import json
import unittest

from evo.widgets.visualization import (
    _as_tileset_ref,
    _assign_glbs_to_collections,
    _collect_collections,
    _iter_content_uris,
    _mark_external_tileset_refs,
    _parse_colormap_detail,
    _parse_nested_tileset,
    _strip_gz_suffix,
    is_visualizable,
)
from evo.widgets import TilesetBundle


class TestSchemaFiltering(unittest.TestCase):
    """Tests for visualisation-service schema support."""

    def test_accepts_supported_object_schema(self) -> None:
        self.assertTrue(is_visualizable("/objects/pointset/1.2.0/pointset.schema.json"))

    def test_rejects_block_models_and_unknown_schemas(self) -> None:
        self.assertFalse(is_visualizable("/objects/block-model/1.0.0/block-model.schema.json"))
        self.assertFalse(is_visualizable("/objects/unknown/1.0.0/unknown.schema.json"))
        self.assertFalse(is_visualizable(None))


class TestColormapParsing(unittest.TestCase):
    """Tests for renderer-facing colormap normalization."""

    def test_parses_continuous_colormap(self) -> None:
        result = _parse_colormap_detail(
            {
                "colors": [[0, 0, 0], [255, 255, 255]],
                "gradient_controls": [0, 1],
                "attribute_controls": [10, 20],
            }
        )
        self.assertEqual(
            result,
            {
                "kind": "continuous",
                "min": 10.0,
                "max": 20.0,
                "stops": [
                    {"position": 0.0, "color": [0.0, 0.0, 0.0]},
                    {"position": 1.0, "color": [1.0, 1.0, 1.0]},
                ],
            },
        )

    def test_parses_category_colormap(self) -> None:
        result = _parse_colormap_detail(
            {"map": ["waste", "ore"], "colors": [[0, 0, 0], [255, 255, 0]]}
        )
        self.assertEqual(
            result,
            {"kind": "category", "map": ["waste", "ore"], "colors": [[0.0, 0.0, 0.0], [1.0, 1.0, 0.0]]},
        )


class TestTilesetContentUrls(unittest.TestCase):
    """Tests for legacy and current 3D Tiles content URL fields."""

    def test_collects_uri_and_legacy_url_content(self) -> None:
        tile = {
            "content": {"url": "legacy.glb.gz"},
            "contents": [{"uri": "current.glb"}, {"url": "additional.glb"}],
            "children": [{"content": {"url": "child.glb"}}],
        }

        self.assertEqual(
            list(_iter_content_uris(tile)),
            ["legacy.glb.gz", "current.glb", "additional.glb", "child.glb"],
        )
        self.assertEqual(_strip_gz_suffix("legacy.glb.gz"), "legacy.glb")


class TestNestedTilesets(unittest.TestCase):
    """Tests for content-based nested-tileset detection and reference rewriting.

    Some services (e.g. downhole collections) reference nested tilesets through
    extension-less URIs such as ``tileset_location``. Detection must be content-based, and
    those references must be rewritten to end in ``.json`` so the browser renderer treats
    them as external tilesets rather than geometry.
    """

    def test_parse_nested_tileset_detects_tileset_json(self) -> None:
        data = json.dumps({"asset": {"version": "1.1"}, "root": {"children": []}}).encode("utf-8")
        self.assertIsNotNone(_parse_nested_tileset(data))

    def test_parse_nested_tileset_rejects_glb_and_plain_json(self) -> None:
        self.assertIsNone(_parse_nested_tileset(b"glTF\x02\x00\x00\x00"))
        self.assertIsNone(_parse_nested_tileset(json.dumps({"not": "a tileset"}).encode("utf-8")))
        self.assertIsNone(_parse_nested_tileset(b"\x00\x01\x02\x03"))

    def test_as_tileset_ref_appends_json_when_missing(self) -> None:
        self.assertEqual(_as_tileset_ref("https://host/path/tileset_location"), "https://host/path/tileset_location.json")
        self.assertEqual(_as_tileset_ref("https://host/path/nested.json"), "https://host/path/nested.json")
        self.assertEqual(_as_tileset_ref("https://host/tileset_x?v=1"), "https://host/tileset_x.json?v=1")

    def test_mark_external_tileset_refs_rewrites_only_known_tilesets(self) -> None:
        root = {
            "children": [
                {"content": {"uri": "https://host/vis/tileset_location"}},
                {"content": {"uri": "https://host/vis/tile_0.glb"}},
            ]
        }
        _mark_external_tileset_refs(root, "https://evo.local/id/tileset.json", {"vis/tileset_location"})
        self.assertEqual(root["children"][0]["content"]["uri"], "https://host/vis/tileset_location.json")
        self.assertEqual(root["children"][1]["content"]["uri"], "https://host/vis/tile_0.glb")


class TestCollections(unittest.TestCase):
    """Tests for grouping downhole geometry into selectable collections.

    Downhole objects publish a collar ``location`` plus one ``collection`` per interval
    table. Each rendered glb embeds only its own attribute hashes, so glbs are matched to
    collections by hash overlap; collar geometry (no embedded attributes) falls back to
    ``Collars`` and non-rendered tables (e.g. depth tables) are dropped.
    """

    def test_collect_collections_returns_empty_without_collections_key(self) -> None:
        self.assertEqual(_collect_collections({"location": {"values": {"data": "aaa"}}}), [])

    def test_collect_collections_includes_collars_and_named_collections(self) -> None:
        object_json = {
            "location": {"coordinates": {"values": {"data": "collar-hash"}}},
            "collections": [
                {"name": "assay", "attributes": [{"values": {"data": "aaa"}}]},
                {"name": "lith", "attributes": [{"values": {"data": "bbb"}}]},
            ],
        }
        defs = _collect_collections(object_json)
        self.assertEqual([d["name"] for d in defs], ["Collars", "assay", "lith"])
        self.assertEqual(defs[0]["hashes"], {"collar-hash"})
        self.assertEqual(defs[1]["hashes"], {"aaa"})
        self.assertEqual(defs[2]["hashes"], {"bbb"})

    def test_assign_glbs_matches_by_hash_and_drops_empty_collections(self) -> None:
        bundle = TilesetBundle(
            object_id="id",
            name="downhole",
            tileset={},
            attributes=[
                {"hash": "aaa", "name": "CU"},
                {"hash": "bbb", "name": "ROCK"},
            ],
        )
        bundle.files = {
            "id/tile_0.glb": b"collar points without attribute hashes",
            "id/tile_1.glb": b"assay glb references aaa somewhere",
            "id/tile_2.glb": b"lith glb references bbb somewhere",
            "id/tileset.json": b"{}",
        }
        collection_defs = [
            {"name": "Collars", "hashes": set()},
            {"name": "assay", "hashes": {"aaa"}},
            {"name": "lith", "hashes": {"bbb"}},
            {"name": "depths", "hashes": {"ccc"}},
        ]
        result = _assign_glbs_to_collections(bundle, collection_defs)
        self.assertEqual([c["name"] for c in result], ["Collars", "assay", "lith"])
        by_name = {c["name"]: c for c in result}
        self.assertEqual(by_name["Collars"]["glbs"], ["id/tile_0.glb"])
        self.assertEqual(by_name["Collars"]["attributes"], [])
        self.assertEqual(by_name["assay"]["glbs"], ["id/tile_1.glb"])
        self.assertEqual(by_name["assay"]["attributes"], ["CU"])
        self.assertEqual(by_name["lith"]["glbs"], ["id/tile_2.glb"])
        self.assertEqual(by_name["lith"]["attributes"], ["ROCK"])