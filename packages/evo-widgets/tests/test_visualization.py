"""Tests for Evo visualisation backend helpers."""

import unittest

from evo.widgets.visualization import _parse_colormap_detail, is_visualizable


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