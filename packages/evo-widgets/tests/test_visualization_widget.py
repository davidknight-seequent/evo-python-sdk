"""Tests for the EvoObjectViewer transport contract."""

import json
import unittest

from evo.widgets import EvoObjectViewer, TilesetBundle


class TestEvoObjectViewer(unittest.TestCase):
    """Tests for packing Python-side tileset bundles for the browser renderer."""

    def test_background_color_can_be_configured(self) -> None:
        viewer = EvoObjectViewer(background_color="#f5f5f5")

        self.assertEqual(viewer.background_color, "#f5f5f5")

    def test_add_bundle_packs_manifest_and_blob(self) -> None:
        viewer = EvoObjectViewer()
        bundle = TilesetBundle(
            object_id="object-1",
            name="Object 1",
            tileset={"asset": {"version": "1.1"}, "root": {}},
            files={"object-1/content.glb": b"content"},
            attributes=[{"name": "Grade", "key": "grade"}],
        )

        self.assertIs(viewer.add_bundle(bundle), viewer)

        manifest = json.loads(viewer._manifest)
        self.assertEqual(manifest["objects"][0]["root"], "https://evo.local/object-1/tileset.json")
        self.assertEqual(manifest["objects"][0]["attributes"], bundle.attributes)
        files = manifest["objects"][0]["files"]
        self.assertEqual(files[0]["offset"], 0)
        self.assertEqual(files[1]["offset"], files[0]["length"])
        self.assertEqual(len(viewer._blob), files[0]["length"] + files[1]["length"])

    def test_clear_removes_packed_bundles(self) -> None:
        viewer = EvoObjectViewer().add_bundle(TilesetBundle("object-1", "Object 1", {"root": {}}))

        self.assertIs(viewer.clear(), viewer)
        self.assertEqual(viewer._blob, b"")
        self.assertEqual(json.loads(viewer._manifest)["objects"], [])