import requests
import math
import json
from dataclasses import dataclass

@dataclass
class TransitousTransitResult:
    travel_time_min: int
    travel_time_max: int
    travel_modes_emojis: str


@dataclass
class TransitousClosestPOIResult:
    travel_time_walk: int
    travel_time_bike: int


emojis = {
    'BIKE': '🚴',
    'BUS': '🚌',
    'WALK': '🚶',
    'REGIONAL_RAIL': '🚝',
    'SUBWAY': '🚇',
    'TRAM': '🚃'
}

class Transitous:
    def __init__(self):
        with open("transitous_config.json", "r") as f:
            config = json.load(f)
        self.user_agent = config['user_agent']

    def get_travel_time_to_work(self, lat: float, lon: float) -> TransitousTransitResult | None:
        url = f'https://api.transitous.org/api/v6/plan?fromPlace={lat:.6f},{lon:.6f}&toPlace=52.374782,4.890170&time=2026-07-17T08%3A07%3A00.000Z&preTransitModes=WALK,BIKE&numItineraries=5&detailedTransfers=false&detailedLegs=false&pedestrianSpeed=1.67&maxTransfers=1'

        headers = {"User-Agent": self.user_agent}

        response = requests.get(url, headers=headers).json()
        results = response['itineraries']
        if len(results) < 1:
            return None

        time_min, time_max = self._get_min_max_durations(results)
        modes = self._get_modes_emojis(results)

        return TransitousTransitResult(time_min, time_max, modes)

    def _get_min_max_durations(self, results):
        times = sorted([r['duration'] for r in results])
        times_in_min = [int(t / 60) for t in times]
        return times_in_min[0], times_in_min[-1]

    def _significant_leg(self, leg):
        return leg['mode'] != "WALK" or leg['duration'] > 180

    def _plan_to_modes(self, plan):
        return [leg['mode'] for leg in plan['legs'] if self._significant_leg(leg)]

    def _get_modes(self, results):
        modes = [self._plan_to_modes(p) for p in results]
        return min(modes, key=lambda k: len(k))

    def _get_modes_emojis(self, results):
        modes = self._get_modes(results)
        return "".join([emojis.get(m, '🚀') for m in modes])

    def get_closest_POI_by_bike_or_walk(self,  lat: float, lon: float, POI_coords: list[tuple]) -> TransitousClosestPOIResult:
        POI_coords_formatted = ','.join(f'{p[0]:.6f};{p[1]:.6f}' for p in POI_coords)
        base_url = f'https://api.transitous.org/api/v1/one-to-many?one={lat:.6f};{lon:.6f}&many={POI_coords_formatted}&max=3600&maxMatchingDistance=200&arriveBy=false'
        headers = {"User-Agent": self.user_agent}

        response_walk = requests.get(base_url + '&mode=WALK', headers=headers).json()
        response_bike = requests.get(base_url + '&mode=BIKE', headers=headers).json()
        min_time_walk_min = int(min(r.get('duration', 3600) for r in response_walk) / 60)
        min_time_bike_min = int(min(r.get('duration', 3600) for r in response_bike) / 60)

        if min_time_walk_min == 60:
            min_time_walk_min = None

        if min_time_bike_min == 60:
            min_time_bike_min = None

        return TransitousClosestPOIResult(min_time_walk_min, min_time_bike_min)
