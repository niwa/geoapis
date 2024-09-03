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
import geopandas

from geoapis import vector


class LrisVectorsTest(unittest.TestCase):
    """A class to test the basic vector.Lris functionality by downloading files from the dataservice.
    The vector attributes are then compared against the expected.

    Tests run include (test_#### indicates the layer tested):
        * test_48556 - Test the specified layer features are correctly downloaded
        * test_48155 - Test the specified layer features are correctly downloaded
    See the associated description for keywords that can be used to search for the layer in the data service.
    """

    # The expected datasets and files to be downloaded - used for comparison in the later tests
    NORTHLAND_EROSION = {
        "area": 2335705.1410131645,
        "geometryType": "Polygon",
        "length": 214953.27371777612,
        "columns": [
            "geometry",
            "OBJECTID",
            "Type",
            "SHAPE_Leng",
            "SHAPE_Area",
            "Confidence",
            "Comment",
            "Age",
            "Area",
            "eros_code",
            "code",
        ],
        "Area": [
            6.78169759532,
            4.15827444502,
            3.12360615684,
            270.951816682,
            21.0797912601,
        ],
    }
    PUKEKOHE_SOILS = {
        "area": 14735624.95558952,
        "geometryType": "Polygon",
        "length": 75736.74547737166,
        "columns": [
            "geometry",
            "DOMSOI",
            "SURCODE",
            "SOIL",
            "SERIES",
            "TOPSOIL_TE",
            "UNIT",
            "NZSC1",
            "NZSC2",
            "NZSC_GROUP",
        ],
        "DOMSOI": ["PhR", "PhR", "Amp", "Am", "Wm"],
    }

    @classmethod
    def setUpClass(cls):
        """Create a cache directory and CatchmentGeometry object for use in the tests and also download the files used
        in the tests."""

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path(
            "tests/test_vector_lris/instruction.json"
        )
        with open(file_path, "r") as file_pointer:
            cls.instructions = json.load(file_pointer)

        # Load in environment variables to get and set the private API keys
        dotenv.load_dotenv()
        lris_key = os.environ.get("LRIS_API", None)
        cls.instructions["instructions"]["apis"]["lris"]["key"] = lris_key

        # define cache location - and catchment dirs
        cls.cache_dir = pathlib.Path(
            cls.instructions["instructions"]["data_paths"]["local_cache"]
        )

        # makes sure the data directory exists but is empty
        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)
        cls.cache_dir.mkdir()

        # Run pipeline - download files
        cls.runner = vector.Lris(
            cls.instructions["instructions"]["apis"]["lris"]["key"],
            crs=cls.instructions["instructions"]["projection"],
            bounding_polygon=None,
            verbose=True,
        )

        cls.runner_generic = vector.WfsQuery(
            key=cls.instructions["instructions"]["apis"]["lris"]["key"],
            crs=cls.instructions["instructions"]["projection"],
            bounding_polygon=None,
            verbose=True,
            netloc_url="lris.scinfo.org.nz",
            geometry_names=["GEOMETRY", "Shape"],
        )

    @classmethod
    def tearDownClass(cls):
        """Remove created cache directory."""

        if cls.cache_dir.exists():
            shutil.rmtree(cls.cache_dir)

    def compare_to_benchmark(
        self,
        features: geopandas.GeoDataFrame,
        benchmark: dict,
        description: str,
        column_name: str,
    ):
        """Compare the features attributes (total area, total length, columns, first five values of a column) against
        those recorded in a benchmark."""

        # check various shape attributes match those expected
        self.assertEqual(
            features.loc[0].geometry.geom_type,
            benchmark["geometryType"],
            "The geometryType of the"
            + f" returned {description} `{features.loc[0].geometry.geom_type}` differs from the "
            + f"expected {benchmark['geometryType']}",
        )
        self.assertEqual(
            list(features.columns),
            benchmark["columns"],
            f"The columns of the returned {description}"
            + f" lines `{list(features.columns)}` differ from the expected {benchmark['columns']}",
        )
        self.assertEqual(
            list(features[column_name][0:5]),
            benchmark[column_name],
            "The value of the 'id' column for "
            + f"the first five entries `{list(features[column_name][0:5])}` differ from the expected "
            + f"{benchmark[column_name]}",
        )
        self.assertEqual(
            features.geometry.area.sum(),
            benchmark["area"],
            f"The area of the returned {description}"
            + f"`{features.geometry.area.sum()}` differs from the expected {benchmark['area']}",
        )
        self.assertEqual(
            features.geometry.length.sum(),
            benchmark["length"],
            "The length of the returned "
            + f"{description} `{features.geometry.length.sum()}` differs from the expected "
            + "{benchmark['length']}",
        )

    def test_48556(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner.run(
            self.instructions["instructions"]["apis"]["lris"]["northland_erosions"][
                "layers"
            ][0]
        )
        description = "Northand Erosion"
        benchmark = self.NORTHLAND_EROSION

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "Area")

    def test_48155(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner.run(
            self.instructions["instructions"]["apis"]["lris"]["pukekohe_soils"][
                "layers"
            ][0]
        )
        description = "Soils in Pukekohe Borough"
        benchmark = self.PUKEKOHE_SOILS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "DOMSOI")

    def test_48556_generic(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner_generic.run(
            self.instructions["instructions"]["apis"]["lris"]["northland_erosions"][
                "layers"
            ][0]
        )
        description = "Northand Erosion"
        benchmark = self.NORTHLAND_EROSION

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "Area")

    def test_48155_generic(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner_generic.run(
            self.instructions["instructions"]["apis"]["lris"]["pukekohe_soils"][
                "layers"
            ][0]
        )
        description = "Soils in Pukekohe Borough"
        benchmark = self.PUKEKOHE_SOILS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "DOMSOI")


if __name__ == "__main__":
    unittest.main()
