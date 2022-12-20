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


class OpenTopographyTestSubfolders(unittest.TestCase):
    """A class to test the basic lidar.OpenTopography functionality by downloading files
    from OpenTopography within a small region. All files are deleted after checking
    their names and size.

    Tests run include:
        1. test_correct_datasets - Test that the expected dataset is downloaded from
        OpenTopography
        2. test_correct_files_downloaded - Test the downloaded LIDAR files have the
        expected names
        3. test_correct_file_sizes - Test the downloaded LIDAR files have the expected
        file sizes
    """

    # The expected datasets and files to be downloaded - used for comparison in the
    # later tests
    DATASETS = [
        "NZ18_Banks",
        "NZ18_AmuriCant",
        "NZ18_Canterbury",
        "Chch_Selwn_2015",
        "Chch_Selwn_2015/NZ_Christchurch",
        "NZ20_Canterbury",
        "NZ20_Cant2",
    ]
    FILE_SIZES = {
        DATASETS[0]: {f"{DATASETS[0]}_TileIndex.zip": 134113},
        DATASETS[1]: {f"{DATASETS[1]}_TileIndex.zip": 813250},
        DATASETS[2]: {
            f"{DATASETS[2]}_TileIndex.zip": 70260,
            "CL2_BX24_2018_1000_2520.laz": 14829064,
        },
        DATASETS[3]: {f"{DATASETS[3]}_TileIndex.zip": 221422},
        DATASETS[4]: {"ot_CL2_BX24_2015_1000_2520.laz": 10761065},
        DATASETS[5]: {
            "CL2_BX24_2020_1000_2520.laz": 25891330,
            f"{DATASETS[5]}_TileIndex.zip": 120930,
        },
        DATASETS[6]: {
            f"{DATASETS[6]}_TileIndex.zip": 1133609,
        },
    }

    @classmethod
    def setUpClass(cls):
        """Create a cache directory and CatchmentGeometry object for use in the tests
        and also download the files used in the tests."""

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path(
            "tests/test_lidar_subfolders/instruction.json"
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
        cls.cache_dir.mkdir(exist_ok=True)

        # create fake catchment boundary
        x0 = 1573300
        x1 = 1573500
        y0 = 5172000
        y1 = 5172200
        catchment = shapely.geometry.Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
        catchment = geopandas.GeoDataFrame(
            geometry=[catchment], crs=instructions["instructions"]["projection"]
        )

        # save faked catchment boundary
        catchment.to_file(cls.cache_dir / "catchment.geojson")

        # Run pipeline - download files
        runner = lidar.OpenTopography(
            cache_path=cls.cache_dir, search_polygon=catchment, verbose=True
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

        dataset_dirs = [self.cache_dir / folder for folder in self.DATASETS]

        # check the right dataset flders were created
        self.assertEqual(
            len(list(self.cache_dir.glob("*/**"))),
            len(dataset_dirs),
            f"There should be {len(dataset_dirs)} dataset folders created (including "
            "nested folders) instead there are "
            f"{len(list(self.cache_dir.glob('*/**')))} created: "
            f"{list(self.cache_dir.glob('*/**'))}",
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
                f"There should be the dataset_files files in dataset {key} instead "
                "there are "
                f"{[file for file in dataset_dir.iterdir() if file.is_file()]}",
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
                "There is a miss-match between the size of the downloaded"
                f" files {[file.stat().st_size for file in dataset_files]}"
                f" and the expected sizes of {self.FILE_SIZES[key].values()}",
            )


if __name__ == "__main__":
    unittest.main()
