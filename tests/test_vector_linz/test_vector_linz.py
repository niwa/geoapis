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

from src.geoapis import vector


class LinzVectorsTest(unittest.TestCase):
    """A class to test the basic vector.Linz functionality by downloading files from the
    dataservice. The vector attributes are then compared against the expected.

    Tests run include (test_#### indicates the layer tested):
        * test_50781 - Test the specified layer features are correctly downloaded
        * test_51572 - Test the specified layer features are correctly downloaded
    See the associated description for keywords that can be used to search for the layer
    in the data service.
    """

    # The expected datasets and files to be downloaded - used for comparison in the
    # later tests
    RAILWAYS = {
        "area": 0.0,
        "geometryType": "MultiLineString",
        "length": 5475052.898111259,
        "columns": ["geometry", "id", "name", "name_utf8"],
        "id": [1775717, 1775718, 1775719, 1778938, 1778939],
    }
    PASTURAL_LEASE = {
        "area": 13463923602.50064,
        "geometryType": "MultiPolygon",
        "length": 15868456.954778664,
        "columns": ["geometry", "id", "lease_name"],
        "id": [12511, 12653, 12658, 12797, 12461],
    }

    @classmethod
    def setUpClass(cls):
        """Create a cache directory and CatchmentGeometry object for use in the tests
        and also download the files used in the tests."""

        # load in the test instructions
        file_path = pathlib.Path().cwd() / pathlib.Path(
            "tests/test_vector_linz/instruction.json"
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

        # Run pipeline - download files
        cls.runner = vector.Linz(
            cls.instructions["instructions"]["apis"]["linz"]["key"],
            crs=cls.instructions["instructions"]["projection"],
            bounding_polygon=None,
            verbose=True,
        )

        cls.runner_generic = vector.WfsQuery(
            key=cls.instructions["instructions"]["apis"]["linz"]["key"],
            crs=cls.instructions["instructions"]["projection"],
            bounding_polygon=None,
            verbose=True,
            netloc_url="data.linz.govt.nz",
            geometry_names=["GEOMETRY", "shape"],
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
        """Compare the features attributes (total area, total length, columns, first
        five values of a column) against those recorded in a benchmark."""

        # check various shape attributes match those expected
        self.assertEqual(
            features.loc[0].geometry.geometryType(),
            benchmark["geometryType"],
            "The geometryType of the returned {description} "
            f"`{features.loc[0].geometry.geometryType()}` differs from the expected "
            f"{benchmark['geometryType']}",
        )
        self.assertEqual(
            list(features.columns),
            benchmark["columns"],
            f"The columns of the returned {description} lines `{list(features.columns)}"
            f"` differ from the expected {benchmark['columns']}",
        )
        self.assertEqual(
            list(features[column_name][0:5]),
            benchmark[column_name],
            "The value of the 'id' column for the first five entries "
            f"`{list(features[column_name][0:5])}` differ from the expected "
            f"{benchmark[column_name]}",
        )
        self.assertEqual(
            features.geometry.area.sum(),
            benchmark["area"],
            f"The area of the returned {description} `{features.geometry.area.sum()}`"
            f" differs from the expected {benchmark['area']}",
        )
        self.assertEqual(
            features.geometry.length.sum(),
            benchmark["length"],
            f"The length of the returned {description} "
            f"`{features.geometry.length.sum()}` differs from the expected "
            f"{benchmark['length']}",
        )

    def test_50781(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner.run(
            self.instructions["instructions"]["apis"]["linz"]["railways"]["layers"][0]
        )
        description = "railways centre lines"
        benchmark = self.RAILWAYS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "id")

    def test_51572(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner.run(
            self.instructions["instructions"]["apis"]["linz"]["pastural_lease"][
                "layers"
            ][0]
        )
        description = "pastural lease parcels"
        benchmark = self.PASTURAL_LEASE

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "id")

    def test_50781_generic(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner_generic.run(
            self.instructions["instructions"]["apis"]["linz"]["railways"]["layers"][0]
        )
        description = "railways centre lines"
        benchmark = self.RAILWAYS

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "id")

    def test_51572_generic(self):
        """Test expected entire layer loaded correctly"""

        features = self.runner_generic.run(
            self.instructions["instructions"]["apis"]["linz"]["pastural_lease"][
                "layers"
            ][0]
        )
        description = "pastural lease parcels"
        benchmark = self.PASTURAL_LEASE

        # check various shape attributes match those expected
        self.compare_to_benchmark(features, benchmark, description, "id")


if __name__ == "__main__":
    unittest.main()
