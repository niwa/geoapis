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

from src.geoapis import vector


class LinzVectorsTest(unittest.TestCase):
    """ A class to test the basic vector.Linz functionality by downloading files from
    OpenTopography within a small region. All files are deleted after checking their names and size.

    Tests run include:
        1. test_land - Test that the expected land dataset is downloaded from LINZ
        2. test_bathymetry - Test that the expected bathymetry dataset is downloaded from LINZ
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    LAND = {"area": 150539169542.3913, "geometryType": 'Polygon', 'length': 6006036.039821965,
            'columns': ['geometry', 'name', 'macronated', 'grp_macron', 'TARGET_FID', 'grp_ascii', 'grp_name',
                        'name_ascii'], 'name': ['South Island or Te Waipounamu']}
    BATHYMETRY_CONTOURS = {"area": 0.0, "geometryType": 'LineString', 'length': 144353.73387463146,
                           'columns': ['geometry', 'fidn', 'valdco', 'verdat', 'inform', 'ninfom', 'ntxtds',
                                       'scamin', 'txtdsc', 'sordat', 'sorind', 'hypcat'],
                           'valdco': [2.0, 2.0, 0.0, 0.0, 0.0, 0.0, 20.0, 0.0, 0.0, 5.0, 10.0, 30.0, 2.0, 0.0]}

    @classmethod
    def setUpClass(cls):
        """ Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests. """

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path("tests/test_vector_in_bounds/instruction.json")
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

        # create fake catchment boundary
        x0 = 1477354
        x1 = 1484656
        y0 = 5374408
        y1 = 5383411
        catchment = shapely.geometry.Polygon([(x0, y0), (x0, y1), (x1, y1), (x1, y0)])
        catchment = geopandas.GeoSeries([catchment])
        catchment = catchment.set_crs(cls.instructions['instructions']['projection'])

        # save faked catchment file
        catchment_dir = cls.cache_dir / "catchment"
        catchment.to_file(catchment_dir)
        shutil.make_archive(base_name=catchment_dir, format='zip', root_dir=catchment_dir)
        shutil.rmtree(catchment_dir)

        # cconvert catchment file to zipfile
        catchment_dir = pathlib.Path(str(catchment_dir) + ".zip")
        catchment_polygon = geopandas.read_file(catchment_dir)
        catchment_polygon.to_crs(cls.instructions['instructions']['projection'])

        # Run pipeline - download files
        cls.runner = vector.Linz(cls.instructions['instructions']['apis']['linz']['key'],
                                 catchment_polygon, verbose=True)

    @classmethod
    def tearDownClass(cls):
        """ Remove created cache directory. """

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def test_land(self):
        """ A test to check expected island is loaded """

        land = self.runner.run(self.instructions['instructions']['apis']['linz']['land']['layers'][0],
                               self.instructions['instructions']['apis']['linz']['land']['geometry_name'])

        # check various shape attributes match those expected
        self.assertEqual(land.loc[0].geometry.geometryType(), self.LAND['geometryType'], "The geometryType of the " +
                         f"returned land polygon `{land.loc[0].geometry.geometryType()}` differs from the expected " +
                         f"{self.LAND['geometryType']}")
        self.assertEqual(list(land.columns), self.LAND['columns'], "The columns of the returned land polygon " +
                         f"`{list(land.columns)}` differ from the expected {self.LAND['columns']}")
        self.assertEqual(list(land['name_ascii']), self.LAND['name'], "The value of the land polygon's 'name' column " +
                         f"`{list(land['name_ascii'])}` differ from the expected {self.LAND['name']}")
        self.assertEqual(land.geometry.area.sum(), self.LAND['area'], "The area of the returned land polygon " +
                         f"`{land.geometry.area.sum()}` differs from the expected {self.LAND['area']}")
        self.assertEqual(land.geometry.length.sum(), self.LAND['length'], "The length of the returned land polygon " +
                         f"`{land.geometry.length.sum()}` differs from the expected {self.LAND['length']}")


    def test_land_no_geometry_name(self):
        """ A test to check expected island is loaded """

        land = self.runner.run(self.instructions['instructions']['apis']['linz']['land']['layers'][0])

        # check various shape attributes match those expected
        self.assertEqual(land.loc[0].geometry.geometryType(), self.LAND['geometryType'], "The geometryType of the " +
                         f"returned land polygon `{land.loc[0].geometry.geometryType()}` differs from the expected " +
                         f"{self.LAND['geometryType']}")
        self.assertEqual(list(land.columns), self.LAND['columns'], "The columns of the returned land polygon " +
                         f"`{list(land.columns)}` differ from the expected {self.LAND['columns']}")
        self.assertEqual(list(land['name_ascii']), self.LAND['name'], "The value of the land polygon's 'name' column " +
                         f"`{list(land['name_ascii'])}` differ from the expected {self.LAND['name']}")
        self.assertEqual(land.geometry.area.sum(), self.LAND['area'], "The area of the returned land polygon " +
                         f"`{land.geometry.area.sum()}` differs from the expected {self.LAND['area']}")
        self.assertEqual(land.geometry.length.sum(), self.LAND['length'], "The length of the returned land polygon " +
                         f"`{land.geometry.length.sum()}` differs from the expected {self.LAND['length']}")


    def test_bathymetry(self):
        """ A test to check expected bathyemtry contours are loaded """

        bathymetry_contours = self.runner.run(
            self.instructions['instructions']['apis']['linz']['bathymetry_contours']['layers'][0],
            self.instructions['instructions']['apis']['linz']['bathymetry_contours']['geometry_name'])

        # check various shape attributes match those expected
        self.assertEqual(bathymetry_contours.loc[0].geometry.geometryType(), self.BATHYMETRY_CONTOURS['geometryType'],
                         "The geometryType of the returned land polygon " +
                         f"`{bathymetry_contours.loc[0].geometry.geometryType()}` differs from the expected " +
                         f"{self.BATHYMETRY_CONTOURS['geometryType']}")
        self.assertEqual(list(bathymetry_contours.columns), self.BATHYMETRY_CONTOURS['columns'], "The columns of the" +
                         f" returned land polygon `{list(bathymetry_contours.columns)}` differ from the expected " +
                         f"{self.BATHYMETRY_CONTOURS['columns']}")
        self.assertEqual(list(bathymetry_contours['valdco']), self.BATHYMETRY_CONTOURS['valdco'], "The columns of the" +
                         f" land polygon's 'valdco' column `{list(bathymetry_contours['valdco'])}` differ from the " +
                         f"expected {self.BATHYMETRY_CONTOURS['valdco']}")
        self.assertEqual(bathymetry_contours.geometry.area.sum(), self.BATHYMETRY_CONTOURS['area'], "The area of the " +
                         f"returned bathymetry_contours polygon `{bathymetry_contours.geometry.area.sum()}` differs " +
                         "from the expected {self.BATHYMETRY_CONTOURS['area']}")
        self.assertEqual(bathymetry_contours.geometry.length.sum(), self.BATHYMETRY_CONTOURS['length'], "The area of " +
                         f"the returned bathymetry_contours polygon `{bathymetry_contours.geometry.length.sum()}` " +
                         "differs from the expected {self.BATHYMETRY_CONTOURS['length']}")


    def test_bathymetry_no_geometry_name(self):
        """ A test to check expected bathyemtry contours are loaded """

        bathymetry_contours = self.runner.run(
            self.instructions['instructions']['apis']['linz']['bathymetry_contours']['layers'][0])

        # check various shape attributes match those expected
        self.assertEqual(bathymetry_contours.loc[0].geometry.geometryType(), self.BATHYMETRY_CONTOURS['geometryType'],
                         "The geometryType of the returned land polygon " +
                         f"`{bathymetry_contours.loc[0].geometry.geometryType()}` differs from the expected " +
                         f"{self.BATHYMETRY_CONTOURS['geometryType']}")
        self.assertEqual(list(bathymetry_contours.columns), self.BATHYMETRY_CONTOURS['columns'], "The columns of the" +
                         f" returned land polygon `{list(bathymetry_contours.columns)}` differ from the expected " +
                         f"{self.BATHYMETRY_CONTOURS['columns']}")
        self.assertEqual(list(bathymetry_contours['valdco']), self.BATHYMETRY_CONTOURS['valdco'], "The columns of the" +
                         f" land polygon's 'valdco' column `{list(bathymetry_contours['valdco'])}` differ from the " +
                         f"expected {self.BATHYMETRY_CONTOURS['valdco']}")
        self.assertEqual(bathymetry_contours.geometry.area.sum(), self.BATHYMETRY_CONTOURS['area'], "The area of the " +
                         f"returned bathymetry_contours polygon `{bathymetry_contours.geometry.area.sum()}` differs " +
                         "from the expected {self.BATHYMETRY_CONTOURS['area']}")
        self.assertEqual(bathymetry_contours.geometry.length.sum(), self.BATHYMETRY_CONTOURS['length'], "The area of " +
                         f"the returned bathymetry_contours polygon `{bathymetry_contours.geometry.length.sum()}` " +
                         "differs from the expected {self.BATHYMETRY_CONTOURS['length']}")


if __name__ == '__main__':
    unittest.main()