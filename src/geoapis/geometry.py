# -*- coding: utf-8 -*-
"""
Created on Tue Aug  3 11:12:56 2021

@author: pearsonra
"""

import geopandas


class TileInfo:
    """A class for working with tiling information"""

    def __init__(
        self, tile_file: str, catchment_polygon: geopandas.geodataframe.GeoDataFrame
    ):
        self._tile_info = geopandas.read_file(tile_file)
        self.catchment_polygon = catchment_polygon

        self.file_name = None
        self.url_name = None

        self._set_up()

    def _set_up(self):
        """Set CRS and select all tiles partially within the catchment, and look up the file column name"""

        if self.catchment_polygon is not None:
            self._tile_info = self._tile_info.to_crs(self.catchment_polygon.crs)
            self._tile_info = geopandas.sjoin(self._tile_info, self.catchment_polygon)
            self._tile_info = self._tile_info.reset_index(drop=True)

        # Try workout the name of the column containing file name information.
        column_names = self._tile_info.columns
        column_name_matches = [
            name for name in column_names if "filename" == name.lower()
        ]
        column_name_matches.extend(
            [name for name in column_names if "file_name" == name.lower()]
        )
        assert len(column_name_matches) == 1, (
            "No single `file name` column detected in the tile file with"
            + f" columns: {column_names}"
        )
        self.file_name = column_name_matches[0]

        # Try workout the name of the column containing URL information.
        column_names = self._tile_info.columns
        column_name_matches = [name for name in column_names if "url" == name.lower()]
        assert len(column_name_matches) == 1, (
            "No single `URL` column detected in the tile file with"
            + f" columns: {column_names}"
        )
        self.url_name = column_name_matches[0]

    @property
    def file_names(self):
        """Return the names of all tiles within the catchment"""

        return self._tile_info[self.file_name]

    @property
    def urls(self):
        """Return the names of all tiles within the catchment"""

        return self._tile_info[self.url_name]
