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
import numpy

from src.geoapis import lidar


class OpenTopographyTest(unittest.TestCase):
    """A class to test the basic lidar.OpenTopography functionality by downloading
    files from OpenTopography within a small region. All files are deleted after
    checking their names and size.

    Tests run include:
        1. test_correct_dataset - Test that the expected dataset is downloaded from
           OpenTopography
        2. test_correct_lidar_files_downloaded - Test the downloaded LIDAR files have
           the expected names
        3. test_correct_lidar_file_size - Test the downloaded LIDAR files have the
           expected file sizes
    """

    # The expected datasets and files to be downloaded - used for comparison in the
    # later tests
    DATASETS = ["Wellington_2013", "NZ21_Kapiti"]
    FILE_SIZES = {
        "ot_CL1_WLG_2013_1km_085033.laz": 6795072,
        "ot_CL1_WLG_2013_1km_086033.laz": 5712485,
        "ot_CL1_WLG_2013_1km_085032.laz": 1670549,
        "ot_CL1_WLG_2013_1km_086032.laz": 72787,
        DATASETS[0] + "_TileIndex.zip": 598532,
    }

    @classmethod
    def setUpClass(cls):
        """Create a cache directory and CatchmentGeometry object for use in the tests
        and also download the files used in the tests."""

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path(
            "tests/test_lidar/instruction.json"
        )
        with open(file_path, "r") as file_pointer:
            instructions = json.load(file_pointer)
        # define cache location - and catchment dirs
        cls.cache_dir = pathlib.Path(
            instructions["instructions"]["data_paths"]["local_cache"]
        )

        # ensure the cache directory doesn't exist - i.e. clean up from last test
        # occurred correctly
        cls.tearDownClass()
        cls.cache_dir.mkdir()

        # create fake catchment boundary
        x0 = 1764410
        y0 = 5470382
        x1 = 1765656
        y1 = 5471702
        catchment = shapely.geometry.Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
        catchment = geopandas.GeoSeries([catchment])
        catchment = catchment.set_crs(instructions["instructions"]["projection"])

        # save faked catchment boundary
        catchment_dir = cls.cache_dir / "catchment"
        catchment.to_file(catchment_dir)
        shutil.make_archive(
            base_name=catchment_dir, format="zip", root_dir=catchment_dir
        )
        shutil.rmtree(catchment_dir)

        # create a catchment_geometry
        catchment_dir = pathlib.Path(str(catchment_dir) + ".zip")
        catchment_polygon = geopandas.read_file(catchment_dir)
        catchment_polygon.to_crs(instructions["instructions"]["projection"])

        # Run pipeline - download files
        runner = lidar.OpenTopography(
            cache_path=cls.cache_dir, search_polygon=catchment_polygon, verbose=True
        )
        runner.run()

    @classmethod
    def tearDownClass(cls):
        """Remove created cache directory and included created and downloaded files at
        the end of the test."""

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def test_correct_datasets(self):
        """A test to see if the correct dataset is downloaded"""

        # check the right dataset is downloaded - self.DATASET
        self.assertEqual(
            len(list(self.cache_dir.glob("*/**"))),
            len(self.DATASETS),
            f"There should only be the datasets {self.DATASETS} instead "
            f"there are {len(list(self.cache_dir.glob('*/**')))} list "
            f"{list(self.cache_dir.glob('*/**'))}",
        )

        self.assertTrue(
            (self.cache_dir / self.DATASETS[0]).exists(),
            f"The {self.DATASETS[0]} directory is missing",
        )
        self.assertTrue(
            (self.cache_dir / self.DATASETS[1]).exists(),
            f"The {self.DATASETS[1]} directory is missing",
        )

    def test_correct_wellington_files_downloaded(self):
        """A test to see if all expected dataset files are downloaded"""

        dataset_dir = self.cache_dir / self.DATASETS[0]
        downloaded_files = [dataset_dir / file for file in self.FILE_SIZES.keys()]

        # check files are correct
        self.assertEqual(
            len(list(dataset_dir.glob("*"))),
            len(downloaded_files),
            f"There should have been {len(downloaded_files)} files downloaded into the"
            f" {self.DATASETS} directory, instead there are "
            f"{len(list(dataset_dir.glob('*')))} files/dirs in the directory",
        )

        self.assertTrue(
            numpy.all([file in downloaded_files for file in dataset_dir.glob("*")]),
            "The downloaded files {list(dataset_dir.glob('*'))} do not match the "
            f"expected files {downloaded_files}",
        )

    def test_correct_file_size(self):
        """A test to see if all expected dataset files are of the right size"""

        dataset_dir = self.cache_dir / self.DATASETS[0]
        downloaded_files = [dataset_dir / file for file in self.FILE_SIZES.keys()]

        # check sizes are correct
        self.assertTrue(
            numpy.all(
                [
                    downloaded_file.stat().st_size
                    == self.FILE_SIZES[downloaded_file.name]
                    for downloaded_file in downloaded_files
                ]
            ),
            "There is a miss-match between the size of the downloaded files"
            f"{[file.stat().st_size for file in downloaded_files]} and the expected "
            f"sizes of {self.FILE_SIZES.values()}",
        )


if __name__ == "__main__":
    unittest.main()
