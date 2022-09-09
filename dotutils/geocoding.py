import numpy as np
import pandas as pd
from geopy import geocoders
from typing import Literal, Optional
import os
from tqdm.auto import tqdm

tqdm.pandas()


class Geocoding:
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
        mapbox_client_key = os.getenv("MAPBOX_API_KEY")

        self.__gmaps_api_key = (
            gmaps_client_key if not google_api_key else google_api_key
        )
        self.__here_api_key = here_client_key if not here_api_key else here_api_key
        self.__mbox_api_key = (
            mapbox_client_key if not mapbox_api_key else mapbox_api_key
        )

    def geocode(
        self,
        address: str = None,
        lat: float = None,
        lng: float = None,
        service: Literal["Google", "Here", "Mapbox", "Nominatim"] = "Google",
        response_type: Literal["geopy", "raw"] = "geopy",
    ) -> Optional[dict]:
        """Geocode addresses or reverse geocode coordinates. Returns the geocode JSON response as a dictionary.
        Choose between passing address param or (lat, lng) params.

        If address parameter is passed, the function assumes natural address geocoding behaviour.
        If lat, lng passed - it assumes reverse geocoding behaviour.
        If all 3 params are passed - it assume natural address geocoding behaviour.

        Args:
            address (str, optional): The address of the place to convert to coordinates. Defaults to None.
            lat (float, optional): The latitude of the coordinate to find address of. Defaults to None.
            lng (float, optional): The longitude of the coordinate to find address of. Defaults to None.
            service (str, optional): Name of service to use for geocoding ('Google', 'Here', 'Mapbox'). Defaults to 'Google'.
            response_type (str, optional): Type of response to return to caller. Can be one of ('geopy', 'raw'). Defaults to geopy.
                geopy returns a geopy object where address, lat, lng, etc can be addressed as an attribute of the geopy object.
                raw returns the service's raw JSON response as if a direct call is made to the geocoding service API.

        Returns:
            dict: The python dictionary format of the JSON response is returned if any.
        """
        try:
            services = dict(
                Google=lambda: geocoders.GoogleV3(self.__gmaps_api_key).geocode(
                    query=address
                )
                if address
                else geocoders.GoogleV3(self.__gmaps_api_key).reverse(query=(lat, lng)),
                Here=lambda: geocoders.Here(apikey=self.__here_api_key).geocode(
                    query=address
                )
                if address
                else geocoders.Here(apikey=self.__here_api_key).reverse(
                    query=(lat, lng)
                ),
                Mapbox=lambda: geocoders.MapBox(api_key=self.__mbox_api_key).geocode(
                    query=address
                )
                if address
                else geocoders.MapBox(api_key=self.__mbox_api_key).reverse(
                    query=(lat, lng)
                ),
                Nominatim=lambda: geocoders.Nominatim(user_agent="tecton").geocode(
                    query=address
                )
                if address
                else geocoders.Nominatim(user_agent="tecton").reverse(query=(lat, lng)),
            )

            response = services[service]()
            if response_type == "geopy":
                return response
            elif response_type == "raw":
                if hasattr(response(), "raw"):
                    return response().raw

        except Exception:
            return None

    def geocode_dataframe(
        self,
        df: pd.DataFrame,
        address_col: str = None,
        lat_col: str = None,
        lng_col: str = None,
        service: Literal["Google", "Here", "Mapbox", "Nominatim"] = "Google",
        raw: bool = True,
    ) -> pd.DataFrame:
        """Geocode a column(s) of a dataframe.

        Args:
            df (pd.DataFrame): The dataframe containing one or more columns to be geocoded.
            address_col (str, optional): The column with the addresses for address geocoding. Defaults to None.
            lat_col (str, optional): The column with latitude of coordinates for reverse geocoding. Defaults to None.
            lng_col (str, optional): The column with longitude of coordinates for reverse geocoding. Defaults to None.
            service (str, optional): Name of service to use for geocoding ('Google', 'Here', 'Mapbox'). Defaults to 'Google'.
            raw (bool, optional): Return full JSON response, else return only essentials of geocoding behaviour. Defaults to True.

        Raises:
            GeocodeCoordinateColumnsNotDiscoverable: If the lat, lng and address columns are not passed and the coordinates cannot be discovered in
                columns of the passed DataFrame, then this error is raised.

        Returns:
            pd.DataFrame: Returns the geocoded dataframe with additional columns, if any.
        """
        if not df.empty:
            if address_col:
                df["geocode_response"] = df[address_col].progress_apply(
                    lambda cell: self.geocode(address=cell, service=service)
                )
                if not raw:
                    df[["geocoded_lat", "geocoded_lng"]] = df["geocode_response"].apply(
                        lambda cell: self.get_coordinates_from_response(cell, service)
                    )
                    del df["geocode_response"]

            else:
                if not (lat_col or lng_col):
                    try:
                        lat_col, lng_col = self.__geospatial.deduce_coordinates(df)
                    except ValueError:
                        raise ValueError("Geocode Coordinate Columns Not Discoverable")

                    df["geocode_response"] = df.progress_apply(
                        lambda row: self.geocode(
                            lat=row.loc[lat_col], lng=row.loc[lng_col]
                        ),
                        axis=1,
                    )
                    if not raw:
                        df["geocoded_address"] = df["geocode_response"].apply(
                            lambda cell: self.get_address_from_response(cell, service)
                        )
                        del df["geocode_response"]
        return df

    def parse_coordinates_from_response(
        self, response: dict, service: str = "Google"
    ) -> tuple:
        """Parse out only geographic coordinates from geocode response (of address geocoding) based on any geocoding service used.

        Args:
            response (dict): The geocode response from any of the services used in geocoding
            service (str, optional): Name of service to use for geocoding ('Google', 'Here', 'Mapbox'). Defaults to 'Google'.

        Returns:
            tuple: The coordinates of the response (from address geocoding)
        """
        if response and type(response) is dict:
            services = {
                "Google": lambda: (
                    response["geometry"]["location"]["lat"],
                    response["geometry"]["location"]["lng"],
                ),
                "Here": lambda: (
                    response["Location"]["NavigationPosition"]["Latitude"],
                    response["Location"]["NavigationPosition"]["Longitude"],
                ),
                "Mapbox": lambda: (
                    response["geometry"]["coordinates"][0],
                    response["geometry"]["coordinates"][1],
                ),
            }
            return services[service]()
        else:
            return (np.nan, np.nan)

    def parse_address_from_response(
        self, response: dict, service: str = "Google"
    ) -> str:
        """Parse out address from geocode response (of reverse geocoding) based on any geocoding service used.

        Args:
            response (dict): The geocode response from any of the services used in geocoding
            service (str, optional): Name of service to use for geocoding ('Google', 'Here', 'Mapbox'). Defaults to 'Google'.

        Returns:
            tuple: The physical address of the response (from reverse geocoding)
        """
        if response and type(response) is dict:
            services = dict(
                Google=lambda: response.get("formatted_address"),
                Here=lambda: response["Location"]["Address"]["Label"],
                Mapbox=lambda: response["properties"]["address"],
            )
            return services[service]()
        else:
            return np.nan
