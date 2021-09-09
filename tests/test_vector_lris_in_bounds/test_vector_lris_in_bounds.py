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


class LrisVectorsTest(unittest.TestCase):
    """ A class to test the basic vector.lris functionality by downloading files from the dataservice within a
    small region. The vector attributes are then compared against the expected.

    Tests run include (test_#### indicates the layer tested):
        * test_105112 - Test the specified layer features are correctly downloaded within the specified bbox
        * test_105112_no_geometry_name - Test the specified layer and bbox, but with no geometry_name given
        * test_104400 - Test the specified layer features are correctly downloaded within the specified bbox
        * test_104400_no_geometry_name - Test the specified layer and bbox, but with no geometry_name given
    See the associated description for keywords that can be used to search for the layer in the data service.
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    NI_PASTURE = {"area": 717394.1067030121, "geometryType": 'Polygon', 'length': 41471.634234214216,
                  'columns': ['geometry', 'cat', 'area_m2', 'low_prod', 'yield_t_ha', 'lcdb_class', 'uid', 'slope',
                              'region', 'Shape_Length', 'Shape_Area'],
                  'uid': ['0900292962', '0900292961', '0900066117', '0900065567', '0900065323']}
    LCDB_V5 = {"area": 89357299.57565206, "geometryType": 'Polygon', 'length': 578730.5152480144,
               'columns': ['geometry', 'Name_2018', 'Name_2012', 'Name_2008', 'Name_2001', 'Name_1996', 'Class_2018',
                           'Class_2012', 'Class_2008', 'Class_2001', 'Class_1996', 'Wetland_18', 'Wetland_12',
                           'Wetland_08', 'Wetland_01', 'Wetland_96', 'Onshore_18', 'Onshore_12', 'Onshore_08',
                           'Onshore_01', 'Onshore_96', 'EditAuthor', 'EditDate', 'LCDB_UID'],
               'Class_2018': [1, 51, 1, 2, 54]}

    @classmethod
    def setUpClass(cls):
        """ Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests. """

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path("tests/test_vector_lris_in_bounds/instruction.json")
        with open(file_path, 'r') as file_pointer:
            cls.instructions = json.load(file_pointer)

        # Load in environment variables to get and set the private API keys
        dotenv.load_dotenv()
        lris_key = os.environ.get('LRIS_API', None)
        cls.instructions['instructions']['apis']['lris']['key'] = lris_key

        # define cache location - and catchment dirs
        cls.cache_dir = pathlib.Path(cls.instructions['instructions']['data_paths']['local_cache'])

        # makes sure the data directory exists but is empty
        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)
        cls.cache_dir.mkdir()

        # create fake catchment boundary
        x0 = 1752000
        x1 = 1753000
        y0 = 5430000
        y1 = 5440000
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
        cls.catchment_polygon = catchment_polygon

        # Run pipeline - download files
        cls.runner = vector.Lris(cls.instructions['instructions']['apis']['lris']['key'], crs=None,
                                 bounding_polygon=catchment_polygon, verbose=True)

    @classmethod
    def tearDownClass(cls):
        """ Remove created cache directory. """

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def compare_to_benchmark(self, features: geopandas.GeoDataFrame, benchmark: dict, description: str,
                             column_name: str):
        """ Compare the features attributes (total area, total length, columns, first five values of a column) against
        those recorded in a benchmark. """

        # check various shape attributes match those expected
        self.assertEqual(features.loc[0].geometry.geometryType(), benchmark['geometryType'], "The geometryType of the" +
                         f" returned {description} `{features.loc[0].geometry.geometryType()}` differs from the " +
                         f"expected {benchmark['geometryType']}")
        self.assertEqual(list(features.columns), benchmark['columns'], f"The columns of the returned {description}" +
                         f" lines `{list(features.columns)}` differ from the expected {benchmark['columns']}")
        self.assertEqual(list(features[column_name][0:5]), benchmark[column_name], "The value of the 'id' column for " +
                         f"the first five entries `{list(features[column_name][0:5])}` differ from the expected " +
                         f"{benchmark[column_name]}")
        self.assertEqual(features.geometry.area.sum(), benchmark['area'], f"The area of the returned {description}" +
                         f"`{features.geometry.area.sum()}` differs from the expected {benchmark['area']}")
        self.assertEqual(features.geometry.length.sum(), benchmark['length'], "The length of the returned " +
                         f"{description} `{features.geometry.length.sum()}` differs from the expected " +
                         "{benchmark['length']}")

    def test_105112(self):
        """ Test expected features of layer loaded """

        features = self.runner.run(
            self.instructions['instructions']['apis']['lris']['ni_pature_productivity']['layers'][0],
            self.instructions['instructions']['apis']['lris']['ni_pature_productivity']['geometry_name'])
        description = "NI pasture productivity"
        benchmark = self.NI_PASTURE

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'uid')

    def test_105112_no_geometry_name(self):
        """ Test expected features of layer loaded without specifying the geometry_name """

        features = self.runner.run(
            self.instructions['instructions']['apis']['lris']['ni_pature_productivity']['layers'][0])
        description = "NI pasture productivity"
        benchmark = self.NI_PASTURE

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'uid')

    def test_104400(self):
        """ Test expected features of layer loaded """

        features = self.runner.run(
            self.instructions['instructions']['apis']['lris']['land_cover_database']['layers'][0],
            self.instructions['instructions']['apis']['lris']['land_cover_database']['geometry_name'])
        description = "Land cover database v5"
        benchmark = self.LCDB_V5

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'Class_2018')

    def test_104400_no_geometry_name(self):
        """ Test expected features of layer loaded without specifying the geometry_name """

        features = self.runner.run(
            self.instructions['instructions']['apis']['lris']['land_cover_database']['layers'][0])
        description = "Land cover database v5"
        benchmark = self.LCDB_V5

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'Class_2018')


if __name__ == '__main__':
    unittest.main()
