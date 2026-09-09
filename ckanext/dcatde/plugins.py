""" register plugin things here """
import json
import logging

import ckan.plugins.toolkit as tk
from ckan import plugins as p
import ckanext.dcatde.commands.cli as cli

log = logging.getLogger(__name__)

# Solr's location_rpt field starts throwing SearchIndexError well before CKAN
# reaches its own limits, so clamp anything above this vertex count to its
# envelope. 500 fits a typical NUTS-3 region outline while still catching
# country-wide MultiPolygons (e.g. BNetzA Ladesäulen ~ 7000 vertices).
_MAX_SPATIAL_VERTICES_DEFAULT = 500
_MAX_SPATIAL_VERTICES_CONFIG_KEY = 'ckanext.dcatde.spatial.max_vertices'


def _max_spatial_vertices():
    return tk.asint(
        tk.config.get(_MAX_SPATIAL_VERTICES_CONFIG_KEY, _MAX_SPATIAL_VERTICES_DEFAULT)
    )


def _iter_coords(node):
    """Yield [x, y] pairs from a GeoJSON coordinates structure of any nesting depth."""
    if not isinstance(node, list) or not node:
        return
    if isinstance(node[0], (int, float)):
        yield node
        return
    for child in node:
        yield from _iter_coords(child)


def _geometries(geojson):
    if geojson.get("type") == "GeometryCollection":
        return geojson.get("geometries") or []
    return [geojson]


def _vertex_count(geojson):
    n = 0
    for g in _geometries(geojson):
        for _ in _iter_coords(g.get("coordinates", [])):
            n += 1
    return n


def _bounds(geojson):
    minx = miny = float("inf")
    maxx = maxy = float("-inf")
    for g in _geometries(geojson):
        for x, y in _iter_coords(g.get("coordinates", [])):
            if x < minx: minx = x
            if x > maxx: maxx = x
            if y < miny: miny = y
            if y > maxy: maxy = y
    if minx == float("inf"):
        return None
    return minx, miny, maxx, maxy


def _envelope_geojson(bounds):
    minx, miny, maxx, maxy = bounds
    return {
        "type": "Polygon",
        "coordinates": [[
            [minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny],
        ]],
    }


def simplify_spatial_if_large(spatial_raw, max_vertices=None):
    """
    Return a simplified GeoJSON envelope string if the GeoJSON in
    `spatial_raw` exceeds `max_vertices`; otherwise None.
    Never raises — bad input is treated as "leave alone".
    """
    if max_vertices is None:
        max_vertices = _max_spatial_vertices()
    try:
        geom = json.loads(spatial_raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(geom, dict) or "type" not in geom:
        return None
    if _vertex_count(geom) <= max_vertices:
        return None
    bounds = _bounds(geom)
    if bounds is None:
        return None
    return json.dumps(_envelope_geojson(bounds))


class DCATdePlugin(p.SingletonPlugin):
    """ Init Plugin """

    p.implements(p.IClick)
    p.implements(p.IPackageController, inherit=True)

    # IClick
    def get_commands(self):
        return cli.get_commands()

    # IPackageController — CKAN 2.10+
    def before_dataset_index(self, pkg_dict):
        raw = pkg_dict.get("spatial")
        if not raw:
            return pkg_dict
        simplified = simplify_spatial_if_large(raw)
        if not simplified:
            return pkg_dict
        pkg_dict["spatial"] = simplified
        log.info(
            "dcatde: replaced spatial for %s with envelope to keep Solr indexing happy",
            pkg_dict.get("name") or pkg_dict.get("id"),
        )
        return pkg_dict

    # CKAN <=2.9 alias
    before_index = before_dataset_index
