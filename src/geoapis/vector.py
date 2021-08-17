# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 10:10:55 2021

@author: pearsonra
"""

import urllib
import requests
import shapely
import shapely.geometry
import geopandas


class Linz:
    """ A class to manage fetching Vector data from LINZ.

    API details at: https://www.linz.govt.nz/data/linz-data-service/guides-and-documentation/wfs-spatial-filtering

    The specified vector layer is queried each time run is called and any vectors passing though the catchment defined
    in the catchment_polygon are returned. 

    Flexibility exists in the inputs. Only the key is required. If no search_polygon is specified all features in a
    layer will be downloaded. If no crs is specified, the search_polygon will be used if the search_polygon is
    specified. If no CRS or search_polygon is specified the CRS of the downloaded features will be used. """

    SCHEME = "https"
    NETLOC_API = "data.linz.govt.nz"
    WFS_PATH_API_START = "/services;key="
    WFS_PATH_API_END = "/wfs"
    LINZ_GEOMETRY_NAMES = ['GEOMETRY', 'shape']

    def __init__(self, key: str, crs: int = None, catchment_polygon: geopandas.geodataframe.GeoDataFrame = None, verbose: bool = False):
        """ Load in vector information from LINZ. Specify the layer to import during run.
        """

        self.key = key
        self.catchment_polygon = catchment_polygon
        self.crs = crs
        self.verbose = verbose

        self._set_up()


    def _set_up(self):
        # Set the crs from the catchment_polygon if it's not been set
        if self.crs is None and self.catchment_polygon is not None:
            self.crs = self.catchment_polygon.crs.to_epsg()

        # Set the catchment_polygon crs from the crs if they differ
        if self.catchment_polygon is not None and self.crs != self.catchment_polygon.crs.to_epsg():
            self.catchment_polygon.to_crs(self.crs)

    def run(self, layer: int, geometry_name: str = ""):
        """ Query for tiles within a catchment for a specified layer and return a list of the vector features names
        within the catchment """

        if self.catchment_polygon is None:
            features = self.get_features(layer)
        else:
            features = self.get_features_inside_catchment(layer, geometry_name)

        return features

    def query_vector_wfs_in_bounds(self, layer: int, bounds, geometry_name: str):
        """ Function to check for tiles in search rectangle using the LINZ WFS vector query API
        https://www.linz.govt.nz/data/linz-data-service/guides-and-documentation/wfs-spatial-filtering

        Note that depending on the LDS layer the geometry_name may be 'shape' - most property/titles,
        or GEOMETRY - most other layers including Hydrographic and Topographic data.

        bounds defines the bounding box containing in the catchment boundary """

        data_url = urllib.parse.urlunparse((self.SCHEME, self.NETLOC_API,
                                            f"{self.WFS_PATH_API_START}{self.key}{self.WFS_PATH_API_END}",
                                            "", "", ""))

        api_query = {
            "service": "WFS",
            "version": 2.0,
            "request": "GetFeature",
            "typeNames": f"layer-{layer}",
            "outputFormat": "json",
            "cql_filter": f"bbox({geometry_name}, {bounds['maxy'].max()}, {bounds['maxx'].max()}, " +
                          f"{bounds['miny'].min()}, {bounds['minx'].min()}, " +
                          f"'urn:ogc:def:crs:{self.catchment_polygon.crs.to_string()}')"
        }

        if self.crs is not None:  # Only specify crs if specified
            api_query["SRSName"] = f"EPSG:{self.crs}"

        response = requests.get(data_url, params=api_query, stream=True)

        return response


    def get_json_response_in_bounds(self, layer: int, bounds, geometry_name: str):
        """ Check for specified `geometry_name` - try the standard LINZ ones in turn if not specified - and check for
        error messages before returning  """

        # If a geometry_name was specified use this, otherwise try the standard LINZ ones
        if geometry_name is not None and geometry_name != "":

            response = self.query_vector_wfs_in_bounds(layer, bounds, geometry_name)
            response.raise_for_status()
            return response.json()

        else:

            # cycle through the standard LINZ geometry_name's - suppress errors and only raise one if no valid responses
            for geometry_name in self.LINZ_GEOMETRY_NAMES:
                response = self.query_vector_wfs_in_bounds(layer, bounds, geometry_name)
                try:
                    response.raise_for_status()
                    return response.json()
                except requests.exceptions.HTTPError:
                    if self.verbose:
                        print(f"Layer: {layer} is not of `geometry_name`: {geometry_name}. URL is: " +\
                              "{requests.Request('POST', data_url, params=params).prepare().url}")
            assert False, f"No geometry types matching that of layer: {layer} tried. The geometry_name's tried are: +" \
                "{geometry_type_list}"


    def get_features_inside_catchment(self, layer: int, geometry_name: str):
        """ Get a list of features within the catchment boundary """

        # radius in metres
        catchment_bounds = self.catchment_polygon.geometry.bounds

        # get feature information from query
        feature_collection = self.get_json_response_in_bounds(layer, catchment_bounds, geometry_name)
        crs = feature_collection['crs']['properties']['name']

        # Cycle through each feature checking in bounds and getting geometry and properties
        features = {'geometry': []}
        for feature in feature_collection['features']:

            shapely_geometry = shapely.geometry.shape(feature['geometry'])

            # check intersection of tile and catchment in LINZ CRS
            if self.catchment_polygon.intersects(shapely_geometry).any():

                # Create column headings for each 'properties' option from the first in-bounds vector
                if len(features['geometry']) == 0:
                    for key in feature['properties'].keys():
                        features[key] = []  # The empty list to append the property values too

                # Convert any one Polygon MultiPolygon to a straight Polygon then add to the geometries
                if (shapely_geometry.geometryType() == 'MultiPolygon' and len(shapely_geometry) == 1):
                    shapely_geometry = shapely_geometry[0]
                features['geometry'].append(shapely_geometry)

                # Add the value of each property in turn
                for key in feature['properties'].keys():
                    features[key].append(feature['properties'][key])

        # Convert to a geopandas dataframe
        if len(features) > 0:
            features = geopandas.GeoDataFrame(features, crs=crs)
        else:
            features = None

        return features


    def query_vector_wfs(self, layer: int):
        """ Function to check for all features associated with a layer using the LINZ WFS vector query API
        https://www.linz.govt.nz/data/linz-data-service/guides-and-documentation/wfs-spatial-filtering """

        data_url = urllib.parse.urlunparse((self.SCHEME, self.NETLOC_API,
                                            f"{self.WFS_PATH_API_START}{self.key}{self.WFS_PATH_API_END}",
                                            "", "", ""))

        api_query = {
            "service": "WFS",
            "version": 2.0,
            "request": "GetFeature",
            "typeNames": f"layer-{layer}",
            "outputFormat": "json"
        }

        if self.crs is not None:  # Only specify crs if specified
            api_query["SRSName"] = f"EPSG:{self.crs}"

        response = requests.get(data_url, params=api_query, stream=True)

        response.raise_for_status()
        return response.json()

    def get_features(self, layer: int):
        """ Get a list of all features associated with layer """

        # get feature information from query
        feature_collection = self.query_vector_wfs(layer)
        crs = feature_collection['crs']['properties']['name']

        # Cycle through each feature checking in bounds and getting geometry and properties
        features = {'geometry': []}
        for feature in feature_collection['features']:

            shapely_geometry = shapely.geometry.shape(feature['geometry'])

            # Create column headings for each 'properties' option from the first in-bounds vector
            if len(features['geometry']) == 0:
                for key in feature['properties'].keys():
                    features[key] = []  # The empty list to append the property values too

            # Convert any one Polygon MultiPolygon to a straight Polygon then add to the geometries
            if (shapely_geometry.geometryType() == 'MultiPolygon' and len(shapely_geometry) == 1):
                shapely_geometry = shapely_geometry[0]
            features['geometry'].append(shapely_geometry)

            # Add the value of each property in turn
            for key in feature['properties'].keys():
                features[key].append(feature['properties'][key])

        # Convert to a geopandas dataframe
        if len(features) > 0:
            features = geopandas.GeoDataFrame(features, crs=crs)
        else:
            features = None

        return features
