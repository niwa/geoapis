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
        * test_land - Test that the expected land dataset is downloaded from LINZ
        * test_land_no_geometry_name - Test that the expected land dataset is downloaded from LINZ even without
                                       specifying the geometery_name
        * test_bathymetry - Test that the expected land dataset is downloaded from LINZ
        * test_bathymetry_no_geometry_name - Test that the expected bathymetry dataset is downloaded from LINZ even
                                             without specifying the geometery_name
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    LAND = {"area": 150539169542.3913, "geometryType": 'Polygon', 'length': 6006036.039821965,
            'columns': ['geometry', 'name', 'macronated', 'grp_macron', 'TARGET_FID', 'grp_ascii', 'grp_name',
                        'name_ascii'], 'name': ['South Island or Te Waipounamu']}
    BATHYMETRY_CONTOURS = {"area": 0.0, "geometryType": 'LineString', 'length': 144353.73387463146,
                           'columns': ['geometry', 'fidn', 'valdco', 'verdat', 'inform', 'ninfom', 'ntxtds',
                                       'scamin', 'txtdsc', 'sordat', 'sorind', 'hypcat'],
                           'valdco': [2.0, 2.0, 0.0, 0.0, 0.0]}

    @classmethod
    def setUpClass(cls):
        """ Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests. """

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path("tests/test_vector_linz_in_bounds/instruction.json")
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

        # Run pipeline - download files
        cls.runner = vector.Linz(cls.instructions['instructions']['apis']['linz']['key'], crs=None,
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

    def test_land(self):
        """ A test to check expected island is loaded """

        features = self.runner.run(self.instructions['instructions']['apis']['linz']['land']['layers'][0],
                                   self.instructions['instructions']['apis']['linz']['land']['geometry_name'])
        description = "1:50k island polygons"
        benchmark = self.LAND

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'name')

    def test_land_no_geometry_name(self):
        """ A test to check expected island is loaded """

        features = self.runner.run(self.instructions['instructions']['apis']['linz']['land']['layers'][0])
        description = "1:50k island polygons"
        benchmark = self.LAND

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'name')

    def test_bathymetry(self):
        """ A test to check expected bathyemtry contours are loaded """

        features = self.runner.run(
            self.instructions['instructions']['apis']['linz']['bathymetry_contours']['layers'][0],
            self.instructions['instructions']['apis']['linz']['bathymetry_contours']['geometry_name'])
        description = "Bathymetry depth contours"
        benchmark = self.BATHYMETRY_CONTOURS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'valdco')

    def test_bathymetry_no_geometry_name(self):
        """ A test to check expected bathyemtry contours are loaded """

        features = self.runner.run(
            self.instructions['instructions']['apis']['linz']['bathymetry_contours']['layers'][0])
        description = "Bathymetry depth contours"
        benchmark = self.BATHYMETRY_CONTOURS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, 'valdco')


if __name__ == '__main__':
    unittest.main()