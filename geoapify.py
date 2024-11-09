import requests
import math
import json

class Geoapify:
    def __init__(self):
        with open("geoapify_config.json", "r") as f:
            config = json.load(f)
        self.api_key = config['api_key']
    
    def get_coordinates(self, address: str, postcode: str) -> tuple[float,float] | None:
        url = f'https://api.geoapify.com/v1/geocode/search?name={address}&postcode={postcode}&country=Netherlands&format=json&apiKey={self.api_key}'

        response = requests.get(url).json()
        results = response['results']
        if len(results) < 1:
            return None
        return (results[0]['lat'], results[0]['lon'])

    def get_map_picture(self, width: int, height: int, center: tuple[float,float], zoom: float, marker: tuple[float,float]) -> bytes:
        url = f'https://maps.geoapify.com/v1/staticmap?style=maptiler-3d&width={width}&height={height}&center=lonlat:{center[1]},{center[0]}&zoom={zoom:.1f}&marker=lonlat:{marker[1]},{marker[0]};color:%23ff0000;size:medium&apiKey={self.api_key}'

        return requests.get(url).content

    def get_amsterdam_center_with_marker(self, marker: tuple[float,float]):
        zoom = self.calculate_zoom_by_map_and_marker(600, 400, (52.368418, 4.890339), marker)
        zoom = min(zoom, 11.5)
        return self.get_map_picture(600, 400, (52.368418, 4.890339), zoom, marker)

    def calculate_zoom_by_map_and_marker(self, width: int, height: int, center: tuple[float,float], marker: tuple[float,float]):
        default_width = 600
        default_height = 400
        default_zoom = 11.5
        default_width_in_m = 10000
        default_height_in_m = 6350

        marker_y = (marker[0], center[1])
        marker_x = (center[0], marker[1])
        dist_y = measure(*marker_y, *center)
        dist_x = measure(*marker_x, *center)

        needed_zoom_x = zoom_calculate(dist_x*2*1.2, width, default_width_in_m, default_width, default_zoom)
        needed_zoom_y = zoom_calculate(dist_y*2*1.2, height, default_height_in_m, default_height, default_zoom)
        return min(needed_zoom_x, needed_zoom_y)


def zoom_calculate(distance_m, distance_px, default_d_m, default_d_px, default_zoom):
    return -(math.log2(distance_m) - math.log2(distance_px * default_d_m / (default_d_px * 2**(-default_zoom))))


def measure(lat1, lon1, lat2, lon2):
    R = 6378.137  # Radius of Earth in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon / 2) * math.sin(dLon / 2)
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c
    return d * 1000  # meters