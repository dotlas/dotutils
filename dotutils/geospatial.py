import flexpolyline
import shapely
from typing import Optional, Union


class Geospatial:
    def __init__(self):
        pass

    def polygon_polyline(
        self, polygon_hash: str = None, shapely_polygon: shapely.geometry.Polygon = None
    ) -> Optional[Union[str, shapely.geometry.Polygon]]:
        if polygon_hash:
            return shapely.geometry.Polygon(flexpolyline.decode(polygon_hash))
        elif shapely_polygon:
            return flexpolyline.encode(list(shapely_polygon.exterior.coords))

    def invert_polygon(
        self, shapely_polygon: shapely.geometry.Polygon
    ) -> Optional[shapely.geometry.base.BaseGeometry]:
        if type(shapely_polygon) is str:
            try:
                shapely_polygon = shapely.wkt.loads(shapely_polygon)
            except Exception:
                return None
        return shapely.ops.transform(lambda x, y: (y, x), shapely_polygon)

    def union_polygons(self, polygons: list) -> Optional[shapely.geometry.Polygon]:
        unified_large_hex = shapely.ops.unary_union(polygons)
        if (
            unified_large_hex.boundary.is_closed
            and type(unified_large_hex) is shapely.geometry.Polygon
        ):
            return unified_large_hex
