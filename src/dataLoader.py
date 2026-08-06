#loads data

import pandas as pd
import numpy as np

import xarray as xr
import geopandas as gpd
import rioxarray as rxr
from shapely.geometry import mapping


class DataLoader:
    ''' Class for loading climate data from differen sources and bring them in the desired format.
    For adding new methods: The result should be a xarray timeseries for your area of interest,
    for one scenario/model/variable combination.'''

    def crop_to_park_boundary(self, nc, boundary, variable):
        ''' Takes an xarray object and crops within the provided boundary (geopandas), returning a clipped xarray object. '''
        # Select just the variable (time_bnds doesn't have spatial dims)
        nc = nc[variable].rio.set_spatial_dims(x_dim="lon", y_dim="lat")
        nc = nc.rio.write_crs("EPSG:4326", inplace=True)
        if nc.rio.crs != boundary.crs:
            boundary = boundary.to_crs(nc.rio.crs)
        nc_clipped = nc.rio.clip(boundary.geometry.values, boundary.crs, drop=True, all_touched=True)

        return(nc_clipped)

    def load_isimip(self, scenario, model, variable, boundary, park):
        ''' Loads data from ISIMIP 3b. This data has also been downloaded, and cropped to the area of interest,
        using bash and CDO. The scripts are in this repository. The data is founds in isimip/data/processed'''
        with xr.open_dataset(f'isimip/data/processed/{model}_w5e5_{scenario}_{variable}_{park}_monthly.nc', engine="netcdf4") as nc:
            nc_clipped = self.crop_to_park_boundary(nc, boundary, variable).mean(("lon", "lat")).load()

        return nc_clipped

    def load_caladapt(self, scenario, model, variable, boundary, park):
        ''' Loads data from Cal-Adapt LOCA2 downscaled climate projections. This data has been
        pre-downloaded and is found in caladapt/. The model string should include the variant
        identifier (e.g. "access-cm2_r1i1p1f1"). '''
        with xr.open_dataset(f'caladapt/{model}_loca2_{scenario}_{variable}_{park}_monthly.nc', engine="netcdf4") as nc:
            nc_clipped = self.crop_to_park_boundary(nc, boundary, variable)
            if 'member_id' in nc_clipped.dims:
                nc_clipped = nc_clipped.squeeze('member_id', drop=True)
            nc_clipped = nc_clipped.mean(('lon', 'lat')).load()

        return nc_clipped
