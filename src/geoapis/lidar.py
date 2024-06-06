# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 10:10:55 2021

@author: pearsonra
"""

import urllib
import pathlib
import logging
import requests
import boto3
import botocore
import botocore.client
import typing
import geopandas
import abc
from tqdm import tqdm
from . import geometry


class S3QueryBase(abc.ABC):
    """A class to manage fetching LiDAR data from Open Topography.

    API details for querying datasets within a search rectangle in a AWS style bucket

    All datasets within a search polygon may be downloaded; or datasets may be selected
    by name (either within a search  polygon or the entire dataset).
    """

    @property
    @abc.abstractmethod
    def NETLOC_DATA():
        """This should be instantiated in the base class. Provide the netloc of the data
        service."""

        raise NotImplementedError("NETLOC_API must be instantiated in the child class")

    @property
    @abc.abstractmethod
    def OT_BUCKET():
        """This should be instantiated in the base class. Provide the netloc of the data
        service."""

        raise NotImplementedError("NETLOC_API must be instantiated in the child class")

    SCHEME = "https"
    NETLOC_API = "portal.opentopography.org"
    PATH_API = "/API/otCatalog"
    OT_CRS = "EPSG:4326"

    def __init__(
        self,
        cache_path: typing.Union[str, pathlib.Path],
        search_polygon: geopandas.geodataframe.GeoDataFrame = None,
        redownload_files: bool = False,
        download_limit_gbytes: typing.Union[int, float] = 100,
        verbose: bool = False,
    ):
        """Define the cache_path (or location where the data will be downloaded). Other
        attributes are optional. If a
        search_polygon is not specified then datasets must be downloaded by name."""

        self.search_polygon = search_polygon
        self.cache_path = pathlib.Path(cache_path)
        self.redownload_files_bool = redownload_files
        self.download_limit_gbytes = download_limit_gbytes
        self.verbose = verbose

        self._dataset_prefixes = None

    def _to_gbytes(self, bytes_number: int):
        """convert bytes into gigabytes"""

        return bytes_number / 1024 / 1024 / 1024

    def run(self, dataset_name: str = None):
        """Download LiDAR dataset(s) either within a search_polygon, by name, or both"""

        if dataset_name is not None:
            self.download_dataset_by_name(dataset_name)
        elif self.search_polygon is not None:
            self.download_datasets_in_polygon()
        else:
            logging.info(
                "Both the search_polygon and dataset_name are None. Either a "
                "dataset_name of search polygon needs to be specified if any datasets "
                "are to be downloaded from OpenTopography. Please specify a "
                "search_polygon during construction, or a dataset_name during run."
            )

    def download_datasets_in_polygon(self):
        """Download all LiDAR datasets within the search polygon"""

        ot_endpoint_url = urllib.parse.urlunparse(
            (self.SCHEME, self.NETLOC_DATA, "", "", "", "")
        )
        client = boto3.client(
            "s3",
            endpoint_url=ot_endpoint_url,
            config=botocore.config.Config(signature_version=botocore.UNSIGNED),
        )

        self._dataset_prefixes = []
        json_response = self.query_for_datasets_inside_catchment()

        # cycle through each dataset within a region
        for json_dataset in json_response["Datasets"]:
            dataset_prefix = json_dataset["Dataset"]["alternateName"]
            self._dataset_prefixes.append(dataset_prefix)

            self.download_dataset(dataset_prefix, client)

    def download_dataset_by_name(self, dataset_name: str):
        """Download a LiDAR dataset by name after checking it is within any specified
        search polygon"""

        ot_endpoint_url = urllib.parse.urlunparse(
            (self.SCHEME, self.NETLOC_DATA, "", "", "", "")
        )
        client = boto3.client(
            "s3",
            endpoint_url=ot_endpoint_url,
            config=botocore.config.Config(signature_version=botocore.UNSIGNED),
        )

        self._dataset_prefixes = []
        if self.search_polygon is None:
            # No search polygon so download the specified dataset
            self._dataset_prefixes.append(dataset_name)
            self.download_dataset(dataset_name, client)
        else:
            # Only download if the specified dataset name is in the search polygon
            json_response = self.query_for_datasets_inside_catchment()
            for json_dataset in json_response["Datasets"]:
                dataset_prefix = json_dataset["Dataset"]["alternateName"]
                if dataset_prefix == dataset_name:
                    self._dataset_prefixes.append(dataset_name)
                    self.download_dataset(dataset_name, client)
                    break

    def query_for_datasets_inside_catchment(self):
        """Function to check for data in search region using the otCatalogue API
        https://portal.opentopography.org/apidocs/#/Public/getOtCatalog"""

        catchment_bounds = self.search_polygon.geometry.to_crs(self.OT_CRS).bounds
        api_query = {
            "productFormat": "PointCloud",
            "minx": catchment_bounds["minx"].min(),
            "miny": catchment_bounds["miny"].min(),
            "maxx": catchment_bounds["maxx"].max(),
            "maxy": catchment_bounds["maxy"].max(),
            "detail": False,
            "outputFormat": "json",
            "inlcude_federated": True,
        }

        data_url = urllib.parse.urlunparse(
            (self.SCHEME, self.NETLOC_API, self.PATH_API, "", "", "")
        )

        response = requests.get(data_url, params=api_query, stream=True)
        response.raise_for_status()
        return response.json()

    def download_dataset(self, dataset_prefix, client):
        """Download all files within an optional search polygon of a given
        dataset_prefix"""

        if self.verbose:
            logging.info(f"Check files in dataset {dataset_prefix}")
        tile_info = self._get_dataset_tile_names(client, dataset_prefix)

        # check download size limit is not exceeded
        lidar_size_bytes = self._calculate_dataset_download_size(
            client, dataset_prefix, tile_info
        )

        assert self._to_gbytes(lidar_size_bytes) < self.download_limit_gbytes, (
            "The size of the LiDAR to be downloaded is "
            f"{self._to_gbytes(lidar_size_bytes)}GB, which greater than the specified "
            f"download limit of {self.download_limit_gbytes}GB. Please free up some and"
            " try again."
        )

        # check for tiles and download as needed
        self._download_tiles_in_catchment(
            client, dataset_prefix, tile_info, lidar_size_bytes
        )

    def _get_dataset_tile_names(self, client, dataset_prefix):
        """Check for the tile index shapefile and download as needed, then load in and
        trim to the catchment to determine which data tiles to download."""

        file_prefix = f"{dataset_prefix}/{dataset_prefix}_TileIndex.zip"
        local_file_path = self.cache_path / file_prefix

        # Download the file if needed
        if self.redownload_files_bool or not local_file_path.exists():

            # ensure folder exists before download
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                client.download_file(self.OT_BUCKET, file_prefix, str(local_file_path))
            except botocore.exceptions.ClientError as e:
                f"An error occured during download {file_prefix}, The error is {e}"
        # load in tile information
        tile_info = geometry.TileInfo(local_file_path, self.search_polygon)

        return tile_info

    def _calculate_dataset_download_size(self, client, _dataset_prefix, tile_info):
        """Sum up the size of the LiDAR data in catchment"""
        lidar_size_bytes = 0

        for tile_url in tile_info.urls:
            # drop the OT_BUCKET from the URL path to get the file_name
            file_name = pathlib.Path(
                *pathlib.Path(urllib.parse.urlparse(tile_url).path).parts[2:]
            )
            local_path = self.cache_path / file_name
            if self.redownload_files_bool or not local_path.exists():

                try:
                    response = client.head_object(
                        Bucket=self.OT_BUCKET, Key=str(file_name.as_posix())
                    )
                    lidar_size_bytes += response["ContentLength"]

                    if self.verbose:
                        logging.info(
                            f"checking size: {file_name}: {response['ContentLength']}"
                            f", total (GB): {self._to_gbytes(lidar_size_bytes)}"
                        )
                except botocore.exceptions.ClientError as e:
                    f"An error occured during access of {file_name}, The error is {e}"
        return lidar_size_bytes

    def _download_tiles_in_catchment(
        self, client, dataset_prefix, tile_info, lidar_size_bytes
    ):
        """Download the LiDAR data within the catchment"""
        total_gb = self._to_gbytes(lidar_size_bytes)
        # Create progress bar context, only displayed if self.verbose is True
        with tqdm(
            disable=not self.verbose, unit="GB", total=total_gb, ncols=140
        ) as progress_bar:
            for url in tile_info.urls:
                self._download_single_tile_in_catchment(
                    client, dataset_prefix, url, progress_bar
                )

    def _download_single_tile_in_catchment(
        self, client, _dataset_prefix, url, progress_bar
    ):
        """Downloads a single LiDAR file"""
        # drop the OT_BUCKET from the URL path to get the file_name
        file_name = pathlib.Path(
            *pathlib.Path(urllib.parse.urlparse(url).path).parts[2:]
        )
        local_path = self.cache_path / file_name

        # ensure folder exists before download - in case its in a subdirectory that
        # hasn't been created yet
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if self.redownload_files_bool or not local_path.exists():
            if self.verbose:
                # Writing to the progress bar instead of printing ensures proper formatting
                progress_bar.write(f"Downloading file: {file_name}")
            try:
                client.download_file(
                    self.OT_BUCKET,
                    str(file_name.as_posix()),
                    str(local_path),
                    Callback=lambda downloaded_bytes: progress_bar.update(
                        self._to_gbytes(downloaded_bytes)
                    ),
                )
            except botocore.exceptions.ClientError as e:
                # Writing to progress bar works even if self.verbose is False. Ensures proper display with bar
                progress_bar.write(
                    f"An error occurred during download {file_name}, The error is {e}"
                )

    @property
    def dataset_prefixes(self):
        """Get the dataset names of all datasets downloaded by this object."""

        assert self._dataset_prefixes is not None, (
            "The run command needs to be called before 'dataset_prefixes' can "
            + "be called."
        )

        return self._dataset_prefixes


class OpenTopography(S3QueryBase):
    """A class to manage fetching LiDAR data from Open Topography

    API details for querying datasets within a search rectangle at:
        https://portal.opentopography.org/apidocs/#/Public/getOtCatalog
    Information for making a `bulk download` of a dataset using the AWS S3 protocol can
    be found by clicking on bulk download under any public dataset.

    All datasets within a search polygon may be downloaded; or datasets may be selected
    by name (either within a search polygon or the entire dataset).
    """

    NETLOC_DATA = "opentopography.s3.sdsc.edu"
    OT_BUCKET = "pc-bulk"
