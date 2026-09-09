# -*- coding: utf8 -*-
"""Tests for spatial-envelope simplification in DCATdePlugin.before_dataset_index."""
import json
import unittest

from ckanext.dcatde.plugins import (
    DCATdePlugin,
    _bounds,
    _vertex_count,
    simplify_spatial_if_large,
)


def _big_multipolygon(polygon_count, verts_per_ring):
    """Build a GeoJSON MultiPolygon with predictable vertex count for testing."""
    polygons = []
    for p in range(polygon_count):
        ring = [[float(p + i), float(p + i)] for i in range(verts_per_ring)]
        polygons.append([ring])
    return {"type": "MultiPolygon", "coordinates": polygons}


SMALL_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [6.98, 51.47], [7.16, 51.47], [7.16, 51.64], [6.98, 51.64], [6.98, 51.47],
    ]],
}


class TestSpatialHelpers(unittest.TestCase):

    def test_vertex_count_polygon(self):
        self.assertEqual(_vertex_count(SMALL_POLYGON), 5)

    def test_vertex_count_geometrycollection(self):
        gc = {"type": "GeometryCollection", "geometries": [SMALL_POLYGON, SMALL_POLYGON]}
        self.assertEqual(_vertex_count(gc), 10)

    def test_bounds_polygon(self):
        self.assertEqual(_bounds(SMALL_POLYGON), (6.98, 51.47, 7.16, 51.64))

    def test_bounds_multi(self):
        mp = _big_multipolygon(3, 10)
        # coords range from 0..12
        minx, miny, maxx, maxy = _bounds(mp)
        self.assertEqual((minx, miny), (0.0, 0.0))
        self.assertEqual((maxx, maxy), (11.0, 11.0))

    def test_bounds_empty(self):
        self.assertIsNone(_bounds({"type": "Polygon", "coordinates": []}))


class TestSimplifySpatialIfLarge(unittest.TestCase):

    def test_small_polygon_untouched(self):
        self.assertIsNone(simplify_spatial_if_large(json.dumps(SMALL_POLYGON)))

    def test_large_multipolygon_replaced_with_envelope(self):
        big = _big_multipolygon(10, 100)  # 1000 vertices
        simplified = simplify_spatial_if_large(json.dumps(big))
        self.assertIsNotNone(simplified)
        env = json.loads(simplified)
        self.assertEqual(env["type"], "Polygon")
        # envelope of 10-poly stack should be [(0,0),(108,0),(108,108),(0,108),(0,0)]
        self.assertEqual(env["coordinates"][0][0], [0.0, 0.0])
        self.assertEqual(env["coordinates"][0][2], [108.0, 108.0])
        self.assertEqual(len(env["coordinates"][0]), 5)

    def test_geometrycollection_over_threshold_replaced(self):
        gc = {
            "type": "GeometryCollection",
            "geometries": [_big_multipolygon(6, 100)],
        }
        simplified = simplify_spatial_if_large(json.dumps(gc))
        self.assertIsNotNone(simplified)
        self.assertEqual(json.loads(simplified)["type"], "Polygon")

    def test_invalid_json_left_alone(self):
        self.assertIsNone(simplify_spatial_if_large("not json{"))

    def test_non_geojson_dict_left_alone(self):
        self.assertIsNone(simplify_spatial_if_large(json.dumps({"foo": "bar"})))

    def test_custom_threshold(self):
        # 5-vertex polygon should trip a max_vertices=4 threshold
        simplified = simplify_spatial_if_large(
            json.dumps(SMALL_POLYGON), max_vertices=4
        )
        self.assertIsNotNone(simplified)


class TestBeforeDatasetIndex(unittest.TestCase):
    """Exercise the plugin method directly without booting the CKAN plugin registry."""

    def setUp(self):
        # DCATdePlugin() would register interfaces globally on import of the CKAN
        # plugin machinery; call the method as an unbound function instead.
        self.hook = DCATdePlugin.before_dataset_index

    def test_no_spatial_extra_is_noop(self):
        pkg = {"id": "abc", "name": "no-spatial"}
        result = self.hook(None, pkg)
        self.assertNotIn("spatial", result)

    def test_small_spatial_passes_through(self):
        pkg = {"id": "abc", "name": "small", "spatial": json.dumps(SMALL_POLYGON)}
        result = self.hook(None, pkg)
        self.assertEqual(json.loads(result["spatial"]), SMALL_POLYGON)

    def test_large_spatial_replaced_with_envelope(self):
        big = _big_multipolygon(10, 100)
        raw = json.dumps(big)
        pkg = {"id": "abc", "name": "bnetza-like", "spatial": raw}
        result = self.hook(None, pkg)
        self.assertEqual(json.loads(result["spatial"])["type"], "Polygon")
        self.assertNotEqual(result["spatial"], raw)

    def test_empty_string_spatial_is_noop(self):
        pkg = {"id": "abc", "name": "empty", "spatial": ""}
        result = self.hook(None, pkg)
        self.assertEqual(result["spatial"], "")


if __name__ == "__main__":
    unittest.main()
