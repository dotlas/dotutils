from time import sleep
from typing import Optional, Literal
import googlemaps
import herepy
import os


class Places:
    def __init__(
        self,
        google_api_key: str = None,
        here_api_key: str = None,
        mapbox_api_key: str = None,
    ):
        """Instantiate geocoding information

        Args:
            google_api_key (str, optional): API Key for Google Maps Platform. Defaults to None.
            here_api_key (str, optional): API Key for HERE Technologies. Defaults to None.
            mapbox_api_key (str,optional) : API Key for Mapbox API. Defaults to None.
        """
        gmaps_client_key = os.getenv("GOOGLE_API_KEY")
        here_client_key = os.getenv("HERE_API_KEY")

        self.__gmaps_api_key = (
            gmaps_client_key if not google_api_key else google_api_key
        )
        self.__here_api_key = here_client_key if not here_api_key else here_api_key

    def radius_places(
        self,
        lat: float,
        lng: float,
        discovery_radius: int = 500,
        search_keyword: str = "restaurant",
        service: Literal["Google", "Here"] = "Google",
    ) -> list:
        """Drop a pin on any place with "lat", "lng". All places of type "search_keyword" are returned in the circle around "discovery_radius".
        Sizing the discovery radius is key as Google will not return more than 60 places in total per pin drop, even if there are more in visible range.

        Args:
            lat (float): The latitude value of the pin-drop
            lng (float): The longitude value of the pin-drop
            discovery_radius (int, optional): The circular radius around which to search for places in meters. Defaults to 500.
            search_keyword (str, optional): A google place type. See https://developers.google.com/maps/documentation/places/web-service/supported_types.
                Defaults to 'restaurant'.

        Returns:
            list: list object of the resulting places a list of dicts.
        """
        max_requests: int = 3
        next_page_token: str = None
        places_results: list[dict] = list()

        if service == "Google":
            while max_requests:
                if next_page_token:
                    sleep(2)
                try:
                    response = googlemaps.Client(
                        key=self.__gmaps_api_key
                    ).places_nearby(
                        location=(lat, lng),
                        radius=discovery_radius,
                        keyword=search_keyword,
                        page_token=next_page_token,
                    )
                except googlemaps.exceptions.ApiError as e:
                    print(e.status)
                    break

                max_requests -= 1

                data = response["results"]

                if data:
                    places_results.extend(data)
                else:
                    break

                if response.get("next_page_token"):
                    next_page_token = response.get("next_page_token")
                else:
                    next_page_token = None
                    break

        elif service == "Here":
            try:
                response = herepy.PlacesApi(
                    api_key=self.__here_api_key
                ).places_in_circle(
                    coordinates=(lat, lng),
                    radius=discovery_radius,
                    query=search_keyword,
                    limit=100,
                )
            except herepy.HEREError as e:
                print(e.message)
            if response and response.as_dict().get("items"):
                places_results.extend(response.as_dict()["items"])

        return places_results

    def google_place_details(
        self, place_id: str, fields: list = None
    ) -> Optional[dict]:
        return googlemaps.Client(key=self.__gmaps_api_key).place(
            place_id=place_id, fields=fields
        )
