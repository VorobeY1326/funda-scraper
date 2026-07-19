import json

from os import listdir
from os.path import isfile, join

from enum import Enum

from geometry import Geometry

AREAS_FOLDER = 'areas'
GREEN_COLOR = 'MediumSeaGreen'
RED_COLOR = 'Red'

class AreaType(Enum):
    GREEN = 1
    ORANGE = 2
    OTHER = 3

class Areas:
    def __init__(self):
        self._load_data()
        self.green_areas = [a['polygon'] for a in self.all_polygons if a['color'] == GREEN_COLOR and a['type'] == 'Polygon']
        self.orange_areas = [a['polygon'] for a in self.all_polygons if a['color'] != GREEN_COLOR and a['type'] == 'Polygon']
        self.points_of_interest = [a['polygon'] for a in self.all_polygons if a['color'] == RED_COLOR and a['type'] == 'Point']
        self.geometry = Geometry()

    def _load_data(self):
        onlyfiles = [f for f in listdir(AREAS_FOLDER) if isfile(join(AREAS_FOLDER, f)) and f.endswith('.geojson')]
        raw_jsons = [self._load_file(f) for f in onlyfiles]
        self.all_polygons = sum((self._load_polygons(j) for j in raw_jsons), [])

    def _load_file(self, filename: str) -> dict:
        with open(join(AREAS_FOLDER, filename), "r") as f:
            return json.load(f)

    def _load_polygons(self, raw_json: dict) -> list[dict]:
        res = []

        for feature in raw_json['features']:
            if not 'geometry' in feature or not 'type' in feature['geometry']:
                continue
            color = feature.get('properties', {}).get('_umap_options', {}).get('color', '')
            res.append({'polygon': feature['geometry'], 'type': feature['geometry']['type'], 'color': color})
        
        return res

    def get_area_type(self, lan: float, lon: float) -> AreaType:
        if self.geometry.is_inside(lan, lon, self.green_areas):
            return AreaType.GREEN
        if self.geometry.is_inside(lan, lon, self.orange_areas):
            return AreaType.ORANGE
        return AreaType.OTHER

    def get_points_of_interest_nearby(self, lan: float, lon: float) -> list[tuple]:
        points = self.geometry.get_points_nearby(lan, lon, self.points_of_interest)
        return [(p['coordinates'][1], p['coordinates'][0]) for p in points]
