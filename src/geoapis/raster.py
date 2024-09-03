# -*- coding: utf-8 -*-
"""
Created on Fri Nov  7 10:10:55 2022

@author: pearsonra
"""

import urllib
import requests
import numpy
import geopandas
import abc
import typing
import pathlib
import logging
import time
import io
import zipfile


class KoordinatesExportsQueryBase(abc.ABC):
    """An abstract class to manage fetching Raster data using the Koordinates exports
    API. Downloads the GeoTiff specified in the run routine.

    API details at: https://help.koordinates.com/site-admin-apis/export-api/

    Parameters
    ----------

    key: str
        The API key.  Must have Search, view and download exported data
        permissions.

    cache_path: pathlib.Path
        The location to download all GeoTiffs queried in the run method.

    crs: int
        The CRS EPSG code for the GeoTifss to be downloaded as.

    bounding_polygon: geopandas.geodataframe.GeoDataFrame
        An option geometry to clip the downloaded GeoTiffs within.

    """

    @property
    @abc.abstractmethod
    def NETLOC_API():
        """This should be instantiated in the base class. Provide the netloc of the data
        service."""

        raise NotImplementedError("NETLOC_API must be instantiated in the child class")

    SCHEME = "https"
    PATH = "services/api/v1"
    PATH_API_END = "/exports"
    K_CRS = "EPSG:4326"

    def __init__(
        self,
        key: str,
        cache_path: typing.Union[str, pathlib.Path],
        crs: int = None,
        bounding_polygon: geopandas.geodataframe.GeoDataFrame = None,
    ):
        """Load in the wfs key and CRS/bounding_polygon if specified. Specify the layer
        to import during run."""

        self.key = key
        self.cache_path = pathlib.Path(cache_path)
        self.bounding_polygon = bounding_polygon
        self.crs = crs

        self.base_url = urllib.parse.urlunparse(
            (
                self.SCHEME,
                self.NETLOC_API,
                self.PATH,
                "",
                "",
                "",
            )
        )

        self._set_up()

    def _set_up(self):
        """Ensure the bouding_polygon and CRS are in agreement."""

        # Set the crs from the bounding_polygon if it's not been set
        if self.crs is None and self.bounding_polygon is not None:
            logging.info("The download CRS is being set from the bounding_polygon")
            self.crs = self.bounding_polygon.crs.to_epsg()
        # Set the bounding_polygon crs from the crs if they differ
        if (
            self.bounding_polygon is not None
            and self.crs != self.bounding_polygon.crs.to_epsg()
        ):
            logging.info(
                "The bounding_polygon is being transformed to the specified "
                "download CRS"
            )
            self.bounding_polygon.to_crs(self.crs)
        # Enforce the bounding_polygon must be a single geometry if it exists
        if self.bounding_polygon is not None:
            self.bounding_polygon = self.bounding_polygon.explode(index_parts=False)
            if not (self.bounding_polygon.type == "Polygon").all():
                logging.warning(
                    "All bounding_polygon parts aren't Polygon's. Ignoring"
                    f" those that aren't {self.bounding_polygon.geometry}"
                )

                self.bounding_polygon = self.bounding_polygon[
                    self.bounding_polygon.type == "Polygon"
                ]
            number_of_coords = sum(
                [
                    len(polygon.coords)
                    for polygon in self.bounding_polygon.explode(
                        index_parts=False
                    ).exterior
                ]
            )
            assert number_of_coords < 1000, (
                "The bounding polygon must be less than 1000 points. Consider using the"
                " bbox to simplify the geometry"
            )

    def run(self, layer: int) -> pathlib.Path:
        """Query for a specified layer and return a geopandas.GeoDataFrame of the vector
        features. If a polygon_boundary is specified, only return vectors passing
        through this polygon."""

        headers = {"Authorization": f"key {self.key}"}

        # Create the initial request
        api_query = {
            "crs": f"EPSG:{self.crs}",
            "formats": {"grid": "image/tiff;subtype=geotiff"},
            "items": [{"item": f"{self.base_url}/layers/{layer}/"}],
        }
        if self.bounding_polygon is not None:
            exterior = self.bounding_polygon.to_crs(self.K_CRS).exterior.loc[0]
            api_query["extent"] = {
                "type": self.bounding_polygon.type.loc[0],
                "coordinates": [list(exterior.coords)],
            }
        logging.info("Send initial request to download image")
        response = requests.post(
            url=f"{self.base_url}/exports/", headers=headers, json=api_query
        )
        json_query = response.json()
        if not json_query["is_valid"]:
            logging.warning(
                "Invalid initial query. Check layer exists and is within bounds. "
                f"json_query['invalid_reasons']: {json_query['invalid_reasons']}. "
                f"json_query['items'][0]['invalid_reasons']: {json_query['items'][0]['invalid_reasons']}"
            )
            return []
        query_id = json_query["id"]

        # Check the state of your exports until the triggered raster exports completes
        logging.info("Check status of download request")
        while True:
            response = requests.get(
                f"{self.base_url}/exports/",
                headers=headers,
            )
            # find the triggered export
            element = [
                element for element in response.json() if element["id"] == query_id
            ][0]
            logging.info(f"/texport state is {element['state']}")
            if element["state"] == "processing":
                logging.info("Not complete - check again in 20s")
                time.sleep(20)
                continue
            elif element["state"] == "complete":
                logging.info("/tCompleted - move to download")
                break
            else:
                logging.warning(
                    f"Could not download raster. Ended with status {element['state']}"
                )
                return []
        # Download the completed export
        logging.info(f"Downloading {element['download_url']} to {self.cache_path}")
        with requests.get(
            element["download_url"],
            headers={"Authorization": f"key {self.key}"},
            stream=True,
        ) as response:
            response.raise_for_status()
            zip_object = zipfile.ZipFile(io.BytesIO(response.content))
            zip_object.extractall(self.cache_path / f"{layer}")
        # Return the file names of the downloaded rasters
        rasters = []
        for file_name in (self.cache_path / f"{layer}").iterdir():
            if file_name.suffix == ".tif":
                rasters.append(file_name)
        return rasters


class Linz(KoordinatesExportsQueryBase):
    """A class to manage fetching Vector data from LINZ.

    LIRS data service can be accessed at: https://https://data.linz.govt.nz/

    Note that only rasters supporting the grid image/tiff geotiff are supported
    """

    NETLOC_API = "data.linz.govt.nz"


class Lris(KoordinatesExportsQueryBase):
    """A class to manage fetching Vector data from LRIS.

    LIRS data service can be accessed at: https://lris.scinfo.org.nz/

    Note that only rasters supporting the grid image/tiff geotiff are supported
    """

    NETLOC_API = "lris.scinfo.org.nz"


class StatsNz(KoordinatesExportsQueryBase):
    """A class to manage fetching Vector data from the Stats NZ datafinder.

    Stats NZ data service can be accessed at: datafinder.stats.govt.nz

    Note that only rasters supporting the grid image/tiff geotiff are supported
    """

    NETLOC_API = "datafinder.stats.govt.nz"


class KoordinatesQuery(KoordinatesExportsQueryBase):
    """A class to manage fetching Vector data from any generic data portal supporting
    WFS.

    Note that the 'geometry_name' used when making a WFS 'cql_filter' queries can vary
    between layers. You will need to specify the 'geometry_name' of the layers you want
    to download.
    """

    def __init__(
        self,
        key: str,
        netloc_url: str,
        crs: int = None,
        bounding_polygon: geopandas.geodataframe.GeoDataFrame = None,
    ):
        """Set NETLOC_API and instantiate the KoordinatesExportsQueryBase"""

        self.netloc_url = netloc_url

        # Setup the WfsQueryBase class
        super(KoordinatesQuery, self).__init__(
            key=key, crs=crs, bounding_polygon=bounding_polygon
        )

    @property
    def NETLOC_API(self):
        """Instantiate the entered netloc of the data service."""

        return self.netloc_url
