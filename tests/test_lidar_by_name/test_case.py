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

from geoapis import lidar


class OpenTopographyTestByName(unittest.TestCase):
    """A class to test the basic lidar.OpenTopography functionality by downloading files from
    OpenTopography within a small region. All files are deleted after checking their names and size.

    Tests run include:
        1. test_correct_datasets - Test that the expected dataset is downloaded from OpenTopography
        2. test_correct_files_downloaded - Test the downloaded LIDAR files have the expected names
        3. test_correct_file_sizes - Test the downloaded LIDAR files have the expected file sizes
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    DATASETS = ["Chch_Selwn_2015", "Chch_Selwn_2015/NZ_Christchurch"]
    FILE_SIZES = {
        DATASETS[0]: {f"{DATASETS[0]}_TileIndex.zip": 221422},
        DATASETS[1]: {"ot_CL2_BX24_2015_1000_2520.laz": 10761065},
    }

    @classmethod
    def setUpClass(cls):
        """Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests."""

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path(
            "tests/test_lidar_by_name/instruction.json"
        )
        with open(file_path, "r") as file_pointer:
            instructions = json.load(file_pointer)

        # define cache location - and catchment dirs
        cls.cache_dir = pathlib.Path(
            instructions["instructions"]["data_paths"]["local_cache"]
        )

        # ensure the cache directory doesn't exist - i.e. clean up from last test occurred correctly
        cls.tearDownClass()
        cls.cache_dir.mkdir(exist_ok=True)

        # create fake catchment boundary
        x0 = 1573300
        x1 = 1573500
        y0 = 5172000
        y1 = 5172200
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
        runner.run(cls.DATASETS[0])

    @classmethod
    def tearDownClass(cls):
        """Remove created cache directory and included created and downloaded files at the end of the test."""

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def test_correct_datasets(self):
        """A test to see if the correct dataset is downloaded"""

        dataset_dirs = [self.cache_dir / folder for folder in self.DATASETS]

        # check the right dataset flders were created
        self.assertEqual(
            len(list(self.cache_dir.glob("*/**"))),
            len(dataset_dirs),
            "There should be "
            + f"{len(dataset_dirs)} dataset folders created (including nested folders) instead there are"
            + f" {len(list(self.cache_dir.glob('*/**')))} created: {list(self.cache_dir.glob('*/**'))}",
        )

        self.assertEqual(
            len(
                [
                    folder
                    for folder in self.cache_dir.rglob("*")
                    if folder.is_dir() and folder in dataset_dirs
                ]
            ),
            len(dataset_dirs),
            f"There should be dataset folders {dataset_dirs} instead there are "
            + f"{list(self.cache_dir.glob('*/**'))}",
        )

    def test_correct_files_downloaded(self):
        """A test to see if all expected dataset files are downloaded"""

        for key in self.FILE_SIZES.keys():
            dataset_dir = self.cache_dir / key
            dataset_files = [dataset_dir / file for file in self.FILE_SIZES[key].keys()]
            self.assertTrue(
                numpy.all(
                    [
                        file in dataset_files
                        for file in dataset_dir.iterdir()
                        if file.is_file()
                    ]
                ),
                f"There should be the dataset_files files in dataset {key} instead there are "
                + f"{[file for file in dataset_dir.iterdir() if file.is_file()]}",
            )

    def test_correct_file_sizes(self):
        """A test to see if all expected dataset files are of the right size"""

        for key in self.FILE_SIZES.keys():
            dataset_dir = self.cache_dir / key
            dataset_files = [dataset_dir / file for file in self.FILE_SIZES[key].keys()]

            # check sizes are correct
            self.assertTrue(
                numpy.all(
                    [
                        dataset_file.stat().st_size
                        == self.FILE_SIZES[key][dataset_file.name]
                        for dataset_file in dataset_files
                    ]
                ),
                "There is a miss-match between the size"
                + f" of the downloaded files {[file.stat().st_size for file in dataset_files]}"
                + f" and the expected sizes of {self.FILE_SIZES[key].values()}",
            )


if __name__ == "__main__":
    unittest.main()
