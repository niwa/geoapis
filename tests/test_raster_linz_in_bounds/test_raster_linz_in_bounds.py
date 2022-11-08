# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 11:11:25 2021

@author: pearsonra
"""

import unittest
import json
import pathlib
import shapely
import geopandas
import shutil
import dotenv
import os
import logging

from src.geoapis import raster


class LinzRasterTest(unittest.TestCase):
    """A class to test the basic raster.Linz functionality by downloading files from
    the dataservice within a small region.

    Tests run include (test_#### indicates the layer tested):
        * test_51768 - Test the specified layer features are correctly downloaded within
          the specified bbox
    See the associated description for keywords that can be used to search for the layer
    in the data service.
    """

    # Datasets and files to be downloaded - used for comparison in the later tests
    RASTER_1 = {"size": 1791242, "name": "MG.tif", "number": 1}

    @classmethod
    def setUpClass(cls):
        """Create a cache directory and CatchmentGeometry object for use in the tests
        and also download the files used in the tests."""

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path(
            "tests/test_raster_linz_in_bounds/instruction.json"
        )
        with open(file_path, "r") as file_pointer:
            cls.instructions = json.load(file_pointer)
        # Load in environment variables to get and set the private API keys
        dotenv.load_dotenv()
        linz_key = os.environ.get("LINZ_API", None)
        cls.instructions["instructions"]["apis"]["linz"]["key"] = linz_key

        # define cache location - and catchment dirs
        cls.cache_dir = pathlib.Path(
            cls.instructions["instructions"]["data_paths"]["local_cache"]
        )

        # makes sure the data directory exists but is empty
        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)
        cls.cache_dir.mkdir()

        # create fake catchment boundary
        x0 = 1477354
        x1 = 1484656
        y0 = 5374408
        y1 = 5383411
        catchment = shapely.geometry.Polygon([(x0, y0), (x0, y1), (x1, y1), (x1, y0)])
        catchment = geopandas.GeoSeries([catchment])
        catchment = catchment.set_crs(cls.instructions["instructions"]["projection"])

        # save faked catchment file
        catchment.to_file(cls.cache_dir / "catchment.geojson")

        # Run pipeline - download files
        cls.runner = raster.Linz(
            key=cls.instructions["instructions"]["apis"]["linz"]["key"],
            crs=None,
            bounding_polygon=catchment,
            cache_path=cls.cache_dir,
        )

    @classmethod
    def tearDownClass(cls):
        """Remove created cache directory."""

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def compare_to_benchmark(
        self, file_names: list, benchmark: dict, description: str,
    ):
        """Compare the various attributes of the raster (total number of files, name,
        file size) against those recorded in a benchmark."""

        # check various raster attributes match those expected
        self.assertEqual(
            len(file_names),
            benchmark["number"],
            f"The number of the downloaded rasters {len(file_names)} does not match the"
            f" expected benchmark number of {benchmark['number']}",
        )
        self.assertEqual(
            file_names[0].name,
            benchmark["name"],
            f"The name of the downloaded raster {file_names[0].name} does not match the"
            f" expected benchmark name of {benchmark['name']}",
        )
        self.assertEqual(
            file_names[0].stat().st_size,
            benchmark["size"],
            f"The size of the downloaded raster {file_names[0].stat().st_size} does not"
            f" match the expected benchmark name of {benchmark['size']}",
        )

    def test_1(self):
        """Test expected features of layer loaded"""

        logging.info("In test_1")
        print(self.instructions)
        file_names = self.runner.run(
            self.instructions["instructions"]["apis"]["linz"]["1"]["layers"],
        )
        benchmark = self.RASTER_1
        description = "nz 8m DEM 2012"
        # check various shape attributes match those expected
        self.compare_to_benchmark(file_names, benchmark, description)


if __name__ == "__main__":
    unittest.main()
