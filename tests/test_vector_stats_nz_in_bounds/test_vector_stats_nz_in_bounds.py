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


class StatsNzVectorsTest(unittest.TestCase):
    """ A class to test the basic vector.StatsNz functionality by downloading files from the dataservice within a
    small region. The vector attributes are then compared against the expected.

    Tests run include (test_#### indicates the layer tested):
        * test_105133 - Test the specified layer features are correctly downloaded within the specified bbox
        * test_105133_no_geometry_name - Test the specified layer and bbox, but with no geometry_name given
        * test_87883 - Test the specified layer features are correctly downloaded within the specified bbox
        * test_87883_no_geometry_name - Test the specified layer and bbox, but with no geometry_name given
    See the associated description for keywords that can be used to search for the layer in the data service.
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    REGIONAL_COUNCILS = {"area": 15945315588.69047, "geometryType": 'Polygon', 'length': 579211.9585905309,
                         'columns': ['geometry', 'REGC2021_V1_00', 'REGC2021_V1_00_NAME', 'REGC2021_V1_00_NAME_ASCII',
                                     'LAND_AREA_SQ_KM', 'AREA_SQ_KM', 'Shape_Length', 'Shape_Area'],
                         'AREA_SQ_KM': [15945.3207507]}
    DISTRICT_HEALTH_BOARDS = {"area": 136597398950.17044, "geometryType": 'MultiPolygon', 'length': 15794952.293961357,
                              'columns': ['geometry', 'DHB2015_Code', 'DHB2015_Name', 'Shape_Length', 'Shape_Area'],
                              'DHB2015_Code': ['14', '99']}

    @classmethod
    def setUpClass(cls):
        """ Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests. """

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path("tests/test_vector_stats_nz_in_bounds/instruction.json")
        with open(file_path, 'r') as file_pointer:
            cls.instructions = json.load(file_pointer)

        # Load in environment variables to get and set the private API keys
        dotenv.load_dotenv()
        lris_key = os.environ.get('STATSNZ_API', None)
        cls.instructions['instructions']['apis']['stats_nz']['key'] = lris_key

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
        cls.runner = vector.StatsNz(cls.instructions['instructions']['apis']['stats_nz']['key'], crs=None,
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

    def test_105133(self):
        """ Test expected features of layer loaded """

        features = self.runner.run(
            self.instructions['instructions']['apis']['stats_nz']['regional_councils']['layers'][0],
            self.instructions['instructions']['apis']['stats_nz']['regional_councils']['geometry_name'])
        description = "Regional Council 2021"
        benchmark = self.REGIONAL_COUNCILS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'AREA_SQ_KM')

    def test_105133_no_geometry_name(self):
        """ Test expected features of layer loaded without specifying the geometry_name """

        features = self.runner.run(
            self.instructions['instructions']['apis']['stats_nz']['regional_councils']['layers'][0])
        description = "Regional Council 2021"
        benchmark = self.REGIONAL_COUNCILS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'AREA_SQ_KM')

    def test_87883(self):
        """ Test expected features of layer loaded """

        features = self.runner.run(
            self.instructions['instructions']['apis']['stats_nz']['district_health_board']['layers'][0],
            self.instructions['instructions']['apis']['stats_nz']['district_health_board']['geometry_name'])
        description = "District Health Board 2015"
        benchmark = self.DISTRICT_HEALTH_BOARDS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'DHB2015_Code')

    def test_87883_no_geometry_name(self):
        """ Test expected features of layer loaded without specifying the geometry_name """

        features = self.runner.run(
            self.instructions['instructions']['apis']['stats_nz']['district_health_board']['layers'][0])
        description = "District Health Board 2015"
        benchmark = self.DISTRICT_HEALTH_BOARDS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'DHB2015_Code')


if __name__ == '__main__':
    unittest.main()
