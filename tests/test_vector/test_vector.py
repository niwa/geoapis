# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 11:11:25 2021

@author: pearsonra
"""

import unittest
import json
import pathlib
import shutil
import dotenv
import os

from src.geoapis import vector


class LinzVectorsTest(unittest.TestCase):
    """ A class to test the basic vector.Linz functionality by downloading files from
    OpenTopography within a small region. All files are deleted after checking their names and size.

    Tests run include:
        1. test_railways - Test that the expected railways dataset is downloaded from LINZ
        2. test_pastural_lease - Test that the expected pastural lease dataset is downloaded from LINZ
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    RAILWAYS = {"area": 0.0, "geometryType": 'MultiLineString', 'length': 5475052.898111259,
                'columns': ['geometry', 'id', 'name', 'name_utf8'], 'id': [1775717, 1775718, 1775719, 1778938, 1778939]}
    PASTURAL_LEASE = {"area": 13387663696.368122, "geometryType": 'MultiPolygon', 'length': 15756644.418670136,
                      'columns': ['geometry', 'id', 'lease_name'], 'id': [12767, 12768, 12770, 12773, 12776]}

    @classmethod
    def setUpClass(cls):
        """ Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests. """

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path("tests/test_vector/instruction.json")
        with open(file_path, 'r') as file_pointer:
            cls.instructions = json.load(file_pointer)

        # Load in environment variables to get and set the private API keys
        dotenv.load_dotenv()
        linz_key = os.environ.get('LINZ_API', None)
        cls.instructions['instructions']['apis']['linz']['key'] = linz_key

        # define cache location - and catchment dirs
        cls.cache_dir = pathlib.Path(cls.instructions['instructions']['data_paths']['local_cache'])

        # makes sure the data directory exists but is empty
        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)
        cls.cache_dir.mkdir()

        # Run pipeline - download files
        cls.runner = vector.Linz(cls.instructions['instructions']['apis']['linz']['key'],
                                 crs=cls.instructions['instructions']['projection'], bounding_polygon=None,
                                 verbose=True)

    @classmethod
    def tearDownClass(cls):
        """ Remove created cache directory. """

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def test_railways(self):
        """ A test to check expected island is loaded """

        features = self.runner.run(self.instructions['instructions']['apis']['linz']['railways']['layers'][0])
        description = "railways centre lines"
        benchmark = self.RAILWAYS

        # check various shape attributes match those expected
        self.assertEqual(features.loc[0].geometry.geometryType(), benchmark['geometryType'], "The geometryType of the" +
                         f" returned {description} `{features.loc[0].geometry.geometryType()}` differs from the " +
                         f"expected {benchmark['geometryType']}")
        self.assertEqual(list(features.columns), benchmark['columns'], "The columns of the returned {description}" +
                         f" lines `{list(features.columns)}` differ from the expected {benchmark['columns']}")
        self.assertEqual(list(features['id'][0:5]), benchmark['id'], "The value of the 'id' column for the first" +
                         f" five entries `{list(features['id'][0:5])}` differ from the expected {benchmark['id']}")
        self.assertEqual(features.geometry.area.sum(), benchmark['area'], "The area of the returned {description}" +
                         f"`{features.geometry.area.sum()}` differs from the expected {benchmark['area']}")
        self.assertEqual(features.geometry.length.sum(), benchmark['length'], "The length of the returned {description}"
                         + f"`{features.geometry.length.sum()}` differs from the expected {benchmark['length']}")

    def test_pastural_lease(self):
        """ A test to check expected island is loaded """

        features = self.runner.run(self.instructions['instructions']['apis']['linz']['pastural_lease']['layers'][0])
        description = "pastural lease parcels"
        benchmark = self.PASTURAL_LEASE

        # check various shape attributes match those expected
        self.assertEqual(features.loc[0].geometry.geometryType(), benchmark['geometryType'], "The geometryType of the" +
                         f" returned {description} `{features.loc[0].geometry.geometryType()}` differs from the " +
                         f"expected {benchmark['geometryType']}")
        self.assertEqual(list(features.columns), benchmark['columns'], "The columns of the returned {description}" +
                         f" lines `{list(features.columns)}` differ from the expected {benchmark['columns']}")
        self.assertEqual(list(features['id'][0:5]), benchmark['id'], "The value of the 'id' column for the first" +
                         f" five entries `{list(features['id'][0:5])}` differ from the expected {benchmark['id']}")
        self.assertEqual(features.geometry.area.sum(), benchmark['area'], "The area of the returned {description}" +
                         f"`{features.geometry.area.sum()}` differs from the expected {benchmark['area']}")
        self.assertEqual(features.geometry.length.sum(), benchmark['length'], "The length of the returned {description}"
                         + f"`{features.geometry.length.sum()}` differs from the expected {benchmark['length']}")


if __name__ == '__main__':
    unittest.main()
