import googlemaps
import herepy
from time import sleep
from typing import Literal, Optional
import os
from dotutils.geospatial import Geospatial
import shapely


class Routing:
    def __init__(self, google_api_key: str = None, here_api_key: str = None):
        """Instantiate places information

        Args:
            google_api_key (str, optional): A Google Maps api key string - will override default key from credentials. Defaults to None.
            here_api_key (str, optional): A HERE api key string - will override default key from credentials. Defaults to None.
        """
        gmaps_client_key = os.getenv("GOOGLE_API_KEY")
        here_client_key = os.getenv("HERE_API_KEY")
        self.__geospatial = Geospatial()

        self.__gmaps_api_key = (
            gmaps_client_key if not google_api_key else google_api_key
        )
        self.__here_api_key = here_client_key if not here_api_key else here_api_key

    def time_coverage(
        self,
        lat: float,
        lng: float,
        mode: Literal["car", "pedastrian", "truck"] = "car",
        minutes: list[int] = [14],
    ) -> list:
        """Takes in a source coordinate and finds the contour lines that represent coverage in 'minutes' from the origin to areas within 'minutes' time.

        Args:
            lat (float): The origin latitude
            lng (float): The origin longitude
            mode (str, optional): Mode of transportation (car, pedastrian, truck). Defaults to 'car'.
            minutes (list, optional): Coverage in minutes in a list. Multiple values return contours. Defaults to [14].

        Returns:
            list: Returns a list of shapely.geometry.Polygon objects
        """

        max_retries: int = 3
        transport_modes = dict(
            car=herepy.here_enum.IsolineRoutingTransportMode.car,
            pedestrian=herepy.here_enum.IsolineRoutingTransportMode.pedestrian,
            truck=herepy.here_enum.IsolineRoutingTransportMode.truck,
        )
        response = None
        while max_retries:
            try:
                response = herepy.IsolineRoutingApi(
                    api_key=self.__here_api_key
                ).time_isoline(
                    transport_mode=transport_modes[mode],
                    origin=[lat, lng],
                    ranges=[int(min * 60) for min in minutes],
                )
                break
            except herepy.HEREError as e:
                sleep(2)
                max_retries -= 1

        routes_list: list[dict] = []

        if response:
            response = response.as_dict()
            for contour in response["isolines"]:
                contour_object = dict(lat=lat, lng=lng, geometry="", time="")

                polygon_hash: str = contour["polygons"][0]["outer"]
                gj_polygon: shapely.geometry.Polygon = self.__geospatial.invert_polygon(
                    self.__geospatial.polygon_polyline(polygon_hash)
                )

                contour_object["geometry"] = gj_polygon.wkt
                contour_object["time"] = int(contour["range"]["value"] / 60)

                routes_list.append(contour_object)

        return routes_list

    def time_coverage_specific_start(
        self,
        lat: float,
        lng: float,
        mode: str = "car",
        time_range: list = [900],
        depart_at: str = "2021-08-14T16:00:00",
    ) -> Optional[dict]:
        response = None
        max_retries = 3

        transport_modes = dict(
            car = herepy.here_enum.IsolineRoutingTransportMode.car,
            pedestrian = herepy.here_enum.IsolineRoutingTransportMode.pedestrian,
            truck = herepy.here_enum.IsolineRoutingTransportMode.truck,
        )

        while max_retries:
            try:
                response = herepy.IsolineRoutingApi(
                    api_key=self.__here_api_key
                ).isoline_routing_at_specific_time(
                    transport_mode=transport_modes[mode],
                    ranges=time_range,
                    departure_time=depart_at,
                    origin=(lat, lng),
                )
                break
            except Exception:
                max_retries -= 1
                sleep(3)

        if response:
            response = response.as_dict()
            for contour in response["isolines"]:
                contour_object = dict(
                    lat = lat,
                    lng = lng,
                    mode = mode,
                    geometry = "",
                    time = "",
                )

                polygon_hash = contour["polygons"][0]["outer"]
                gj_polygon = self.__geospatial.invert_polygon(
                    self.__geospatial.polygon_polyline(polygon_hash)
                )

                contour_object["geometry"] = gj_polygon.wkt
                contour_object["time"] = int(contour["range"]["value"] / 60)

                return [contour_object]

    def route(
        self,
        source_lat: float,
        source_lng: float,
        dest_lat: float,
        dest_lng: float,
        transport_mode: str = "driving",
    ):
        return googlemaps.Client(key=self.__gmaps_api_key).directions(
            origin=[source_lat, source_lng],
            destination=[dest_lat, dest_lng],
            mode=transport_mode,
            units="metric",
            departure_time="",
            traffic_model="best_guess",
        )
