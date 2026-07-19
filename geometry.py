from shapely import Point
from shapely.geometry import mapping, shape

class Geometry:
    def __init__(self):
        pass

    def is_inside(self, point_lan: float, point_lon: float, geojson_polygons: list[dict]):
        point = Point(point_lon, point_lan)

        polygons = [shape(p) for p in geojson_polygons]
        return any(point.within(p) for p in polygons)
    
    def get_points_nearby(self, point_lan: float, point_lon: float, geojson_points: list[dict]):
        point = Point(point_lon, point_lan)
        # Hack to not introduce proper projections; very approximately 5km, walking distance
        circle = point.buffer(0.06)

        return [p for p in geojson_points if shape(p).within(circle)]