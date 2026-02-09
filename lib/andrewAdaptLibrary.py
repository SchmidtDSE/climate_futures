"""
andrewAdaptLibrary — Reusable Climate Data Utilities for Cal-Adapt / LOCA2
==========================================================================

Single-file library for fetching, processing, and exporting downscaled CMIP6
climate data from the Cal-Adapt Analytics Engine (https://cal-adapt.org).

Every function here is extracted from battle-tested benchmark notebooks
(TestDirectS3, TestCoiled, 01_fetch_climate_data).

Usage:
    import sys, os
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "lib"))
    from andrewAdaptLibrary import CatalogExplorer, load_boundary, get_climate_data
"""

# ===========================================================================
# Section 1: Imports
# ===========================================================================

import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import rioxarray  # noqa: F401 — registers .rio accessor on xarray objects
import fsspec
import os
import warnings
from typing import Optional

# Lazy imports for optional heavy dependencies
# (avoids ImportError if coiled/dask not installed)
_dask = None
_coiled = None


def _get_dask():
    global _dask
    if _dask is None:
        import dask
        _dask = dask
    return _dask


# ===========================================================================
# Section 2: Constants & Human-Readable Mappings
# ===========================================================================

STANDARD_CRS = "EPSG:4326"
CATALOG_URL = "https://cadcat.s3.amazonaws.com/cae-zarr.csv"
S3_BUCKET = "s3://cadcat"

# Maps between human-readable short names and CMIP6/LOCA2 identifiers
VARIABLE_MAP = {
    "T_Max": {
        "id": "tasmax",
        "ck_name": "Maximum air temperature at 2m",
        "units_raw": "K",
        "units_final": "C",
    },
    "T_Min": {
        "id": "tasmin",
        "ck_name": "Minimum air temperature at 2m",
        "units_raw": "K",
        "units_final": "C",
    },
    "Precip": {
        "id": "pr",
        "ck_name": "Precipitation (total)",
        "units_raw": "kg/m^2/s",
        "units_final": "mm/month",
    },
}

# Reverse lookup: variable_id -> short name
VARIABLE_MAP_REV = {v["id"]: k for k, v in VARIABLE_MAP.items()}

SCENARIO_MAP = {
    "Historical Climate": "historical",
    "SSP 2-4.5": "ssp245",
    "SSP 3-7.0": "ssp370",
    "SSP 5-8.5": "ssp585",
}

# Reverse lookup: experiment_id -> friendly name
SCENARIO_MAP_REV = {v: k for k, v in SCENARIO_MAP.items()}

# Default processing parameters (can be overridden per-call)
DEFAULT_BASELINE = (1995, 2014)
DEFAULT_TIMESPAN = (1950, 2100)
DEFAULT_SMOOTHING_WINDOW = 10
HISTORICAL_END_YEAR = 2014

# Temporal resolution mapping (user-friendly -> table_id)
# Note: Hourly data (1hr) is only available for WRF, not LOCA2
TIMESCALE_MAP = {
    "monthly": "mon",
    "daily": "day",
    "yearly": "yrmax",
    # Also allow raw table_id values
    "mon": "mon",
    "day": "day",
    "yrmax": "yrmax",
}

# Documentation of what each timescale means and how it's aggregated
# Based on standard CMIP6/LOCA2 conventions
TIMESCALE_INFO = {
    "monthly": {
        "table_id": "mon",
        "description": "Monthly aggregated values",
        "aggregation": {
            "temperature": "Monthly mean of daily values",
            "precipitation": "Monthly sum of daily values (mm/month)",
            "humidity": "Monthly mean of daily values",
            "wind": "Monthly mean of daily values",
            "radiation": "Monthly mean of daily values",
        },
        "variables_available": 10,  # All LOCA2 variables
    },
    "daily": {
        "table_id": "day",
        "description": "Daily values (native LOCA2 resolution)",
        "aggregation": {
            "tasmax": "Daily maximum temperature",
            "tasmin": "Daily minimum temperature",
            "pr": "Daily total precipitation",
            "humidity/wind/radiation": "Daily mean values",
        },
        "variables_available": 10,  # All LOCA2 variables
        "note": "~30x more data than monthly. Use for extreme event analysis.",
    },
    "yearly": {
        "table_id": "yrmax",
        "description": "Annual maximum values",
        "aggregation": {
            "tasmax": "Maximum daily Tmax in the year (hottest day)",
        },
        "variables_available": 1,  # Only tasmax
        "note": "Only tasmax available. Use for annual extreme heat analysis.",
    },
}

# Variables available at each timescale for LOCA2 d03 (3km)
TIMESCALE_VARIABLES = {
    "monthly": ["tasmax", "tasmin", "pr", "hursmax", "hursmin", "huss", "rsds", "uas", "vas", "wspeed"],
    "daily": ["tasmax", "tasmin", "pr", "hursmax", "hursmin", "huss", "rsds", "uas", "vas", "wspeed"],
    "yearly": ["tasmax"],  # Only annual max temperature
}

# Standard CSV columns for output
CSV_COLUMNS = [
    "Year", "Region", "Scenario", "DataScenario",
    "Simulation", "Variable", "Anomaly",
]


def validate_timescale_variables(variables: list, timescale: str) -> None:
    """Check that requested variables are available at the given timescale.

    Args:
        variables: List of variable short names (e.g. ["T_Max", "T_Min", "Precip"])
        timescale: Temporal resolution ("monthly", "daily", "yearly")

    Raises:
        ValueError: If any variable is not available at the requested timescale
    """
    available = TIMESCALE_VARIABLES.get(timescale, [])
    if not available:
        raise ValueError(
            f"Unknown timescale: {timescale!r}. "
            f"Options: {list(TIMESCALE_VARIABLES.keys())}"
        )

    unavailable = []
    for var_key in variables:
        var_id = VARIABLE_MAP.get(var_key, {}).get("id", var_key)
        if var_id not in available:
            unavailable.append(var_key)

    if unavailable:
        # Build helpful error message
        available_friendly = [
            VARIABLE_MAP_REV.get(v, v) for v in available
        ]
        timescale_info = TIMESCALE_INFO.get(timescale, {})

        msg = (
            f"Variable(s) {unavailable} not available at timescale={timescale!r}.\n"
            f"Available at {timescale}: {available_friendly}\n"
        )
        if timescale == "yearly":
            msg += (
                "Note: 'yearly' only has annual maximum temperature (T_Max).\n"
                "For other variables, use 'monthly' or 'daily'."
            )
        raise ValueError(msg)


def get_timescale_info(timescale: str = None) -> dict:
    """Get information about available timescales and their aggregation methods.

    Args:
        timescale: Specific timescale to get info for, or None for all

    Returns:
        dict with timescale info including aggregation methods
    """
    if timescale is not None:
        return TIMESCALE_INFO.get(timescale, {})
    return TIMESCALE_INFO


# ===========================================================================
# Section 3: CatalogExplorer — Live Data Discovery
# ===========================================================================

class CatalogExplorer:
    """Discover what climate data is available on Cal-Adapt at runtime.

    All results come from the live S3 catalog CSV — never hardcoded.
    Defaults to LOCA2 monthly 3km grid (Statistical downscaling).

    Example:
        cat = CatalogExplorer()
        print(cat.variables())
        print(cat.scenarios())
        print(cat.summary())

        # For daily data:
        cat_daily = CatalogExplorer(timescale="daily")
    """

    def __init__(self, activity: str = "LOCA2", timescale: str = "monthly",
                 grid: str = "d03", table: str = None):
        """Load and filter the Cal-Adapt catalog CSV from S3.

        Args:
            activity: Activity ID filter (default "LOCA2" for statistical downscaling)
            timescale: Temporal resolution - "monthly", "daily", "hourly", or "yearly"
                       (default "monthly")
            grid: Grid label filter (default "d03" for 3km)
            table: DEPRECATED - use timescale instead. Raw table_id if you must.
        """
        # Handle deprecated 'table' parameter
        if table is not None:
            import warnings
            warnings.warn(
                "CatalogExplorer(table=...) is deprecated. Use timescale= instead. "
                "Options: 'monthly', 'daily', 'hourly', 'yearly'",
                DeprecationWarning
            )
            table_id = table
        else:
            table_id = TIMESCALE_MAP.get(timescale)
            if table_id is None:
                raise ValueError(
                    f"Unknown timescale: {timescale!r}. "
                    f"Options: {list(TIMESCALE_MAP.keys())}"
                )

        self._raw = pd.read_csv(CATALOG_URL)
        self._filtered = self._raw[
            (self._raw.activity_id == activity)
            & (self._raw.table_id == table_id)
            & (self._raw.grid_label == grid)
        ].copy()
        self._activity = activity
        self._table = table_id
        self._timescale = timescale
        self._grid = grid

    @property
    def catalog_size(self) -> int:
        """Total number of Zarr stores matching the filter."""
        return len(self._filtered)

    def variables(self) -> dict:
        """Available variable_ids with human-readable names where known.

        Returns:
            dict mapping variable_id -> friendly name (or variable_id if unknown)
            e.g. {"tasmax": "T_Max", "tasmin": "T_Min", "pr": "Precip", "huss": "huss"}
        """
        var_ids = sorted(self._filtered.variable_id.unique())
        return {vid: VARIABLE_MAP_REV.get(vid, vid) for vid in var_ids}

    def scenarios(self) -> dict:
        """Available experiment_ids with human-readable names where known.

        Returns:
            dict mapping experiment_id -> friendly name
            e.g. {"historical": "Historical Climate", "ssp245": "SSP 2-4.5"}
        """
        exp_ids = sorted(self._filtered.experiment_id.unique())
        return {eid: SCENARIO_MAP_REV.get(eid, eid) for eid in exp_ids}

    def gcms(self, variable_id: Optional[str] = None,
             experiment_id: Optional[str] = None) -> list:
        """Available GCMs (source_ids), optionally filtered.

        Args:
            variable_id: Filter by variable (e.g. "tasmax")
            experiment_id: Filter by experiment (e.g. "historical")

        Returns:
            Sorted list of GCM source_id strings
        """
        df = self._filtered
        if variable_id is not None:
            df = df[df.variable_id == variable_id]
        if experiment_id is not None:
            df = df[df.experiment_id == experiment_id]
        return sorted(df.source_id.unique())

    def time_range(self, variable_id: str, experiment_id: str,
                   source_id: Optional[str] = None) -> tuple:
        """Actual (min_year, max_year) by opening a sample Zarr store.

        Opens one Zarr store to read time coordinate bounds. This makes a
        network call (~1-2s) but returns 100% accurate time range.

        Args:
            variable_id: e.g. "tasmax"
            experiment_id: e.g. "historical"
            source_id: Optional specific GCM. If None, picks the first available.

        Returns:
            (min_year, max_year) tuple of integers
        """
        paths = self.s3_paths(variable_id, experiment_id)
        if not paths:
            raise ValueError(
                f"No data found for variable={variable_id}, "
                f"experiment={experiment_id}"
            )

        if source_id is not None:
            match = [p for p in paths if p["source_id"] == source_id]
            if not match:
                raise ValueError(f"source_id={source_id} not found")
            sample_path = match[0]["path"]
        else:
            sample_path = paths[0]["path"]

        store = fsspec.get_mapper(sample_path, anon=True)
        ds = xr.open_zarr(store, consolidated=True)
        time_vals = ds.time.values
        min_year = int(pd.Timestamp(time_vals[0]).year)
        max_year = int(pd.Timestamp(time_vals[-1]).year)
        ds.close()
        return (min_year, max_year)

    def s3_paths(self, variable_id: str,
                 experiment_id: str) -> list[dict]:
        """All S3 Zarr store paths for a variable + experiment combo.

        Args:
            variable_id: e.g. "tasmax"
            experiment_id: e.g. "historical"

        Returns:
            List of dicts: [{"source_id": ..., "member_id": ..., "path": ...}, ...]
        """
        rows = self._filtered[
            (self._filtered.variable_id == variable_id)
            & (self._filtered.experiment_id == experiment_id)
        ]
        return rows[["source_id", "member_id", "path"]].to_dict("records")

    def summary(self) -> pd.DataFrame:
        """Cross-tab of variables x experiments showing simulation counts.

        Returns:
            DataFrame with variable_ids as rows, experiment_ids as columns,
            values = number of simulations (Zarr stores).
        """
        ct = self._filtered.groupby(
            ["variable_id", "experiment_id"]
        ).size().reset_index(name="count")
        return ct.pivot_table(
            index="variable_id", columns="experiment_id",
            values="count", fill_value=0
        ).astype(int)

    def validate_mappings(self) -> dict:
        """Cross-check VARIABLE_MAP and SCENARIO_MAP against live catalog.

        Returns:
            {"valid": bool, "issues": [str, ...]}
        """
        issues = []
        live_vars = set(self._filtered.variable_id.unique())
        live_exps = set(self._filtered.experiment_id.unique())

        for vk, vinfo in VARIABLE_MAP.items():
            if vinfo["id"] not in live_vars:
                issues.append(
                    f"VARIABLE_MAP['{vk}'] -> '{vinfo['id']}' "
                    f"not in catalog"
                )

        for sk, sid in SCENARIO_MAP.items():
            if sid not in live_exps:
                issues.append(
                    f"SCENARIO_MAP['{sk}'] -> '{sid}' not in catalog"
                )

        return {"valid": len(issues) == 0, "issues": issues}

    @property
    def timescale(self) -> str:
        """Current temporal resolution setting."""
        return self._timescale

    def __repr__(self) -> str:
        return (
            f"CatalogExplorer(activity={self._activity!r}, "
            f"timescale={self._timescale!r}, grid={self._grid!r}, "
            f"stores={self.catalog_size})"
        )


# ===========================================================================
# Section 4: Boundary Helpers
# ===========================================================================

def load_boundary(shapefile_path: str) -> gpd.GeoDataFrame:
    """Load a shapefile and reproject to EPSG:4326.

    Args:
        shapefile_path: Path to .shp file

    Returns:
        GeoDataFrame in WGS84 (EPSG:4326)
    """
    gdf = gpd.read_file(shapefile_path)
    if gdf.crs is None or str(gdf.crs) != STANDARD_CRS:
        gdf = gdf.to_crs(STANDARD_CRS)
    return gdf


def get_lat_lon_bounds(boundary: gpd.GeoDataFrame) -> tuple:
    """Extract lat/lon bounding box from a GeoDataFrame.

    Args:
        boundary: GeoDataFrame in EPSG:4326

    Returns:
        ((lat_min, lat_max), (lon_min, lon_max))
    """
    b = boundary.total_bounds  # [minx, miny, maxx, maxy]
    lat_bounds = (b[1], b[3])
    lon_bounds = (b[0], b[2])
    return lat_bounds, lon_bounds


def boundary_to_wkt(boundary: gpd.GeoDataFrame) -> tuple:
    """Serialize boundary for Coiled workers (GeoDataFrames can't pickle).

    Args:
        boundary: GeoDataFrame

    Returns:
        (wkt_list, crs_string) — both are plain Python types, safe to send
        to remote workers via dask.delayed
    """
    wkt_list = boundary.geometry.to_wkt().tolist()
    crs_string = str(boundary.crs)
    return wkt_list, crs_string


def boundary_from_wkt(wkt_list: list, crs: str) -> gpd.GeoDataFrame:
    """Reconstruct a GeoDataFrame from WKT strings (on a remote worker).

    Args:
        wkt_list: List of WKT geometry strings
        crs: CRS string (e.g. "EPSG:4326")

    Returns:
        GeoDataFrame
    """
    from shapely import wkt as shapely_wkt
    geometries = [shapely_wkt.loads(w) for w in wkt_list]
    return gpd.GeoDataFrame(geometry=geometries, crs=crs)


# ===========================================================================
# Section 5: Preprocessing
# ===========================================================================

def detect_spatial_dims(da: xr.DataArray) -> tuple:
    """Detect the (y_dim, x_dim) names in a DataArray.

    LOCA2 data uses 'lat'/'lon', climakitae may use 'latitude'/'longitude'.

    Returns:
        (y_dim, x_dim) tuple of strings

    Raises:
        ValueError if spatial dims can't be identified
    """
    y_dim = None
    x_dim = None

    for candidate in ("latitude", "lat", "y"):
        if candidate in da.dims:
            y_dim = candidate
            break

    for candidate in ("longitude", "lon", "x"):
        if candidate in da.dims:
            x_dim = candidate
            break

    if y_dim is None or x_dim is None:
        raise ValueError(
            f"Could not identify spatial dimensions. Dims: {da.dims}"
        )
    return y_dim, x_dim


def convert_units(da: xr.DataArray, var_key: str) -> xr.DataArray:
    """Convert raw data units to analysis-ready units.

    - Temperature (T_Max, T_Min, T_Avg): Kelvin -> Celsius
    - Precipitation: kg/m^2/s -> mm/month

    Args:
        da: Input DataArray
        var_key: One of "T_Max", "T_Min", "T_Avg", "Precip"

    Returns:
        DataArray with converted units
    """
    if "T_" in var_key:
        # Kelvin -> Celsius (only if still in K)
        if da.attrs.get("units") == "K" or (da.values.size > 0 and float(da.mean()) > 100):
            da = da - 273.15
            da.attrs["units"] = "C"
    elif var_key == "Precip":
        raw_units = da.attrs.get("units", "")
        if raw_units in ("kg/m^2/s", "kg m-2 s-1") or "kg" in str(raw_units):
            days_in_month = da.time.dt.days_in_month
            da = da * 86400 * days_in_month
            da.attrs["units"] = "mm/month"
    return da


def setup_spatial_metadata(da: xr.DataArray) -> xr.DataArray:
    """Write CRS and set spatial dimensions via rioxarray.

    Args:
        da: DataArray with lat/lon coordinates

    Returns:
        DataArray with CRS and spatial dims set
    """
    if da.rio.crs is None:
        da = da.rio.write_crs(STANDARD_CRS)

    y_dim, x_dim = detect_spatial_dims(da)
    da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
    return da


def preprocess(da: xr.DataArray, var_key: str) -> xr.DataArray:
    """Full preprocessing pipeline: unit conversion + spatial metadata.

    Args:
        da: Raw DataArray from fetch
        var_key: Variable short name ("T_Max", "T_Min", "Precip")

    Returns:
        Preprocessed DataArray ready for spatial operations
    """
    da = convert_units(da, var_key)
    da = setup_spatial_metadata(da)
    return da


# ===========================================================================
# Section 6: Spatial Processing
# ===========================================================================

def clip_to_boundary(da: xr.DataArray,
                     boundary: gpd.GeoDataFrame) -> xr.DataArray:
    """Clip a DataArray to a boundary polygon.

    Handles CRS alignment and coordinate cleanup before clipping.

    Args:
        da: DataArray with rioxarray spatial metadata set
        boundary: GeoDataFrame with boundary polygon(s)

    Returns:
        Clipped DataArray (pixels outside boundary are NaN)
    """
    y_dim, x_dim = detect_spatial_dims(da)

    # Drop redundant coord aliases to avoid rioxarray confusion
    coords_to_drop = []
    if x_dim != "lon" and "lon" in da.coords:
        coords_to_drop.append("lon")
    if y_dim != "lat" and "lat" in da.coords:
        coords_to_drop.append("lat")
    da = da.drop_vars(coords_to_drop, errors="ignore")

    # Ensure spatial dims are last (rioxarray requirement)
    spatial_dims = (y_dim, x_dim)
    non_spatial = [d for d in da.dims if d not in spatial_dims]
    expected_order = tuple(non_spatial) + spatial_dims
    if da.dims != expected_order:
        da = da.transpose(*expected_order)

    # CRS alignment
    if str(da.rio.crs) != str(boundary.crs):
        boundary = boundary.to_crs(da.rio.crs)

    return da.rio.clip(
        boundary.geometry.values,
        drop=False,
        all_touched=True,
    )


def cosine_weighted_spatial_mean(da: xr.DataArray) -> xr.DataArray:
    """Compute cosine-latitude-weighted spatial mean.

    Accounts for the fact that grid cells at different latitudes represent
    different physical areas.

    Args:
        da: DataArray with spatial dimensions

    Returns:
        DataArray with spatial dimensions collapsed
    """
    y_dim, x_dim = detect_spatial_dims(da)
    weights = np.cos(np.deg2rad(da[y_dim]))
    weights.name = "weights"

    try:
        result = da.weighted(weights).mean(dim=[x_dim, y_dim], skipna=True)
    except Exception:
        warnings.warn("Weighted average failed, falling back to unweighted mean")
        result = da.mean(dim=[x_dim, y_dim], skipna=True)

    return result


def spatial_average(da: xr.DataArray,
                    boundary: gpd.GeoDataFrame) -> xr.DataArray:
    """Full spatial processing: load -> clip -> cosine-weighted mean.

    Calls .load() internally to materialize the data before clipping.

    Args:
        da: Preprocessed DataArray (lazy or loaded)
        boundary: GeoDataFrame with boundary polygon(s)

    Returns:
        DataArray with spatial dimensions collapsed, loaded in memory
    """
    loaded = da.load()
    clipped = clip_to_boundary(loaded, boundary)
    avg = cosine_weighted_spatial_mean(clipped)
    avg.load()

    if avg.isnull().all():
        warnings.warn("spatial_average returned all NaN — check boundary overlap")
        return None
    return avg


# ===========================================================================
# Section 7: Temporal Processing & Anomalies
# ===========================================================================

def annual_aggregate(da: xr.DataArray, var_key: str) -> xr.DataArray:
    """Resample monthly data to annual.

    - Temperature: annual mean
    - Precipitation: annual sum

    Args:
        da: Monthly DataArray (spatial dims already collapsed)
        var_key: "T_Max", "T_Min", "T_Avg", or "Precip"

    Returns:
        Annual DataArray
    """
    if var_key == "Precip":
        annual = da.resample(time="YE").sum(dim="time", skipna=True)
        annual.attrs["units"] = "mm/year"
    else:
        annual = da.resample(time="YE").mean(dim="time", skipna=True)
    return annual


def compute_anomalies(annual: xr.DataArray, var_key: str,
                      baseline: tuple = DEFAULT_BASELINE) -> xr.DataArray:
    """Compute anomalies relative to a baseline period.

    - Temperature: absolute difference (degrees C)
    - Precipitation: percent change (%), or absolute if baseline < 1 mm/yr

    Args:
        annual: Annual DataArray
        var_key: Variable short name
        baseline: (start_year, end_year) for baseline period

    Returns:
        Anomaly DataArray
    """
    baseline_slice = annual.sel(
        time=slice(str(baseline[0]), str(baseline[1]))
    )

    if baseline_slice.time.size == 0:
        warnings.warn(
            f"No data in baseline period {baseline}. Returning raw data."
        )
        return annual

    baseline_mean = baseline_slice.mean(dim="time")

    if var_key == "Precip":
        if (np.abs(baseline_mean) < 1.0).any():
            # Very low baseline precip — use absolute difference
            anomalies = annual - baseline_mean
        else:
            anomalies = ((annual - baseline_mean) / baseline_mean) * 100
    else:
        anomalies = annual - baseline_mean

    return anomalies


def smooth(da: xr.DataArray, window: int = DEFAULT_SMOOTHING_WINDOW) -> xr.DataArray:
    """Apply centered rolling mean for temporal smoothing.

    Args:
        da: Input DataArray with time dimension
        window: Rolling window size (default 10 years)

    Returns:
        Smoothed DataArray
    """
    return da.rolling(time=window, center=True, min_periods=1).mean()


def compute_t_avg(t_max: xr.DataArray,
                  t_min: xr.DataArray) -> xr.DataArray:
    """Compute average temperature from T_Max and T_Min.

    Args:
        t_max: Maximum temperature DataArray
        t_min: Minimum temperature DataArray

    Returns:
        Average temperature DataArray
    """
    t_avg = (t_max + t_min) / 2
    t_avg.attrs = t_max.attrs.copy()
    t_avg.attrs["long_name"] = "Average air temperature at 2m"
    return t_avg


# ===========================================================================
# Section 8: Data Fetching Backends
# ===========================================================================

def _open_one_zarr(s3_path: str) -> xr.Dataset:
    """Open a single Zarr store from S3 with anonymous access.

    Args:
        s3_path: Full S3 path (e.g. "s3://cadcat/loca2/ucsd/...")

    Returns:
        Lazy xarray Dataset
    """
    store = fsspec.get_mapper(s3_path, anon=True)
    return xr.open_zarr(store, consolidated=True)


def fetch_direct_s3(var_key: str, experiment: str,
                    time_slice: tuple, lat_bounds: tuple,
                    lon_bounds: tuple,
                    catalog: Optional[CatalogExplorer] = None) -> xr.DataArray:
    """Fetch climate data directly from S3 Zarr stores.

    Bypasses climakitae entirely. Opens all Zarr stores for the given
    variable + experiment, slices to time/space, concats along simulation dim.

    Thread-safe — can be called from multiple threads simultaneously.

    Args:
        var_key: Variable short name ("T_Max", "T_Min", "Precip")
        experiment: Experiment ID ("historical", "ssp245", etc.)
        time_slice: (start_year, end_year)
        lat_bounds: (south, north)
        lon_bounds: (west, east)
        catalog: Optional CatalogExplorer (created if not provided)

    Returns:
        Lazy DataArray with dims (simulation, time, lat, lon)
    """
    if catalog is None:
        catalog = CatalogExplorer()

    var_id = VARIABLE_MAP[var_key]["id"]
    paths = catalog.s3_paths(var_id, experiment)

    if not paths:
        raise ValueError(
            f"No S3 paths found for {var_key} ({var_id}) / {experiment}"
        )

    time_sel = slice(str(time_slice[0]), str(time_slice[1]))
    datasets = []

    for info in paths:
        ds = _open_one_zarr(info["path"])
        da = ds[var_id]
        da = da.sel(time=time_sel)

        # Spatial slice
        lat_dim = "lat" if "lat" in da.dims else "latitude"
        lon_dim = "lon" if "lon" in da.dims else "longitude"
        da = da.sel(
            **{
                lat_dim: slice(lat_bounds[0], lat_bounds[1]),
                lon_dim: slice(lon_bounds[0], lon_bounds[1]),
            }
        )

        # Add simulation identifier
        sim_name = f"LOCA2_{info['source_id']}_{info['member_id']}"
        da = da.expand_dims(simulation=[sim_name])
        datasets.append(da)

    combined = xr.concat(datasets, dim="simulation")
    return combined


def fetch_climakitae(var_key: str, scenario: str,
                     time_slice: tuple, lat_bounds: tuple,
                     lon_bounds: tuple,
                     timescale: str = "monthly") -> xr.DataArray:
    """Fetch climate data via climakitae's get_data().

    WARNING: NOT thread-safe! Call sequentially only.

    Args:
        var_key: Variable short name ("T_Max", "T_Min", "Precip")
        scenario: Friendly scenario name ("Historical Climate", "SSP 2-4.5", etc.)
        time_slice: (start_year, end_year)
        lat_bounds: (south, north)
        lon_bounds: (west, east)
        timescale: "monthly", "daily", "hourly", or "yearly" (default "monthly")

    Returns:
        Lazy DataArray
    """
    from climakitae.core.data_interface import get_data

    var_name = VARIABLE_MAP[var_key]["ck_name"]

    # Map our timescale names to climakitae's expected values
    ck_timescale_map = {
        "monthly": "monthly",
        "daily": "daily",
        "hourly": "hourly",
        "yearly": "yearly",
    }
    ck_timescale = ck_timescale_map.get(timescale, timescale)

    da = get_data(
        variable=var_name,
        resolution="3 km",
        downscaling_method="Statistical",
        timescale=ck_timescale,
        scenario=[scenario],
        time_slice=time_slice,
        latitude=lat_bounds,
        longitude=lon_bounds,
    )

    if da is None or da.time.size == 0:
        raise ValueError(
            f"climakitae returned no data for {var_key} / {scenario}"
        )
    return da


def build_coiled_task(var_key: str, var_id: str, paths_list: list,
                      time_slice: tuple, lat_bounds: tuple,
                      lon_bounds: tuple, boundary_wkt: list,
                      boundary_crs: str):
    """Create a @dask.delayed task that runs on a Coiled worker.

    The worker opens Zarr stores from S3 at datacenter speed, does the full
    pipeline (unit convert, load, clip, spatial avg), and returns a small
    pandas DataFrame.

    Args:
        var_key: Variable short name
        var_id: CMIP6 variable_id (e.g. "tasmax")
        paths_list: List of {"source_id":..., "member_id":..., "path":...}
        time_slice: (start_year, end_year)
        lat_bounds: (south, north)
        lon_bounds: (west, east)
        boundary_wkt: WKT strings from boundary_to_wkt()
        boundary_crs: CRS string from boundary_to_wkt()

    Returns:
        dask.delayed object that computes to a pandas DataFrame
    """
    dask = _get_dask()

    @dask.delayed
    def _remote_process(var_key, var_id, paths_list, time_slice_start,
                        time_slice_end, lat_bounds, lon_bounds,
                        boundary_wkt, boundary_crs):
        """Runs ENTIRELY on a Coiled worker in us-west-2."""
        import xarray as xr
        import numpy as np
        import pandas as pd
        import geopandas as gpd
        import rioxarray  # noqa: F401
        from shapely import wkt
        import fsspec
        import time as _time

        t0 = _time.perf_counter()
        time_sel = slice(str(time_slice_start), str(time_slice_end))

        # Reconstruct boundary from WKT
        geometries = [wkt.loads(w) for w in boundary_wkt]
        boundary_gdf = gpd.GeoDataFrame(geometry=geometries, crs=boundary_crs)

        # Open all Zarr stores
        datasets = []
        for info in paths_list:
            store = fsspec.get_mapper(info["path"], anon=True)
            ds = xr.open_zarr(store, consolidated=True)
            da = ds[var_id]
            da = da.sel(time=time_sel)

            lat_dim = "lat" if "lat" in da.dims else "latitude"
            lon_dim = "lon" if "lon" in da.dims else "longitude"
            da = da.sel(
                **{
                    lat_dim: slice(lat_bounds[0], lat_bounds[1]),
                    lon_dim: slice(lon_bounds[0], lon_bounds[1]),
                }
            )

            sim_name = f"LOCA2_{info['source_id']}_{info['member_id']}"
            da = da.expand_dims(simulation=[sim_name])
            datasets.append(da)

        t_open = _time.perf_counter() - t0
        combined = xr.concat(datasets, dim="simulation")

        # Unit conversion
        if var_key in ("T_Max", "T_Min", "T_Avg"):
            combined = combined - 273.15
        elif var_key == "Precip":
            days = combined.time.dt.days_in_month
            combined = combined * 86400 * days

        # CRS setup
        if combined.rio.crs is None:
            combined = combined.rio.write_crs("EPSG:4326")
        lat_dim = "lat" if "lat" in combined.dims else "latitude"
        lon_dim = "lon" if "lon" in combined.dims else "longitude"
        combined = combined.rio.set_spatial_dims(
            x_dim=lon_dim, y_dim=lat_dim
        )

        # Load (FAST — worker is in us-west-2 near S3)
        t_load_start = _time.perf_counter()
        loaded = combined.load()
        t_load = _time.perf_counter() - t_load_start

        # Clip to boundary
        masked = loaded.rio.clip(
            boundary_gdf.geometry.values, all_touched=True, drop=False
        )

        # Cos-weighted spatial average
        weights = np.cos(np.deg2rad(masked[lat_dim]))
        weights.name = "weights"
        avg = masked.weighted(weights).mean(
            dim=[lon_dim, lat_dim], skipna=True
        )

        t_total = _time.perf_counter() - t0

        # Convert to DataFrame for transfer back
        df = avg.to_dataframe(name=var_key).reset_index()
        df["_open_time"] = t_open
        df["_load_time"] = t_load
        df["_total_time"] = t_total

        return df

    return _remote_process(
        var_key, var_id, paths_list,
        time_slice[0], time_slice[1],
        lat_bounds, lon_bounds,
        boundary_wkt, boundary_crs,
    )


# ===========================================================================
# Section 9: High-Level API
# ===========================================================================

def get_climate_data(
    variables: list,
    scenarios: list,
    boundary: gpd.GeoDataFrame,
    time_slice: tuple = DEFAULT_TIMESPAN,
    timescale: str = "monthly",
    backend: str = "direct_s3",
    coiled_cluster=None,
    catalog: Optional[CatalogExplorer] = None,
) -> dict:
    """Fetch climate data for all variable x scenario combinations.

    This is the main entry point for data retrieval.

    Args:
        variables: List of variable short names (e.g. ["T_Max", "T_Min", "Precip"])
        scenarios: List of friendly scenario names (e.g. ["Historical Climate", "SSP 2-4.5"])
        boundary: GeoDataFrame with study area boundary
        time_slice: (start_year, end_year)
        timescale: Temporal resolution - "monthly", "daily", or "yearly" (default "monthly")
            - "monthly": Monthly aggregates. Temperature = mean, Precip = sum.
            - "daily": Daily values. ~30x more data than monthly.
            - "yearly": Annual max only. Only T_Max available.
        backend: "direct_s3" | "climakitae" | "coiled"
        coiled_cluster: Required if backend="coiled" — a coiled.Cluster or dask Client
        catalog: Optional CatalogExplorer (created automatically if needed)

    Returns:
        dict keyed by variable name -> DataFrame
        For "coiled" backend: {variable: DataFrame} where each DataFrame has columns:
            [time, simulation, scenario, timescale, <variable_name>]
        All scenarios are concatenated into a single DataFrame per variable.
        Example: data["T_Max"] returns a DataFrame with all requested scenarios.
        The timescale column indicates "monthly", "daily", or "yearly".
        For other backends: {variable: xr.DataArray} (preprocessed)

    Raises:
        ValueError: If requested variables are not available at the given timescale.
            For example, timescale="yearly" only supports T_Max.
    """
    # Validate that requested variables exist at this timescale
    validate_timescale_variables(variables, timescale)

    lat_bounds, lon_bounds = get_lat_lon_bounds(boundary)

    if catalog is None and backend in ("direct_s3", "coiled"):
        catalog = CatalogExplorer(timescale=timescale)
    elif catalog is not None and catalog.timescale != timescale:
        # User passed a catalog but with different timescale - warn them
        import warnings
        warnings.warn(
            f"Provided catalog has timescale={catalog.timescale!r} but "
            f"timescale={timescale!r} was requested. Using catalog's timescale."
        )

    results = {}

    if backend == "direct_s3":
        for scenario in scenarios:
            experiment = SCENARIO_MAP.get(scenario, scenario)
            for var_key in variables:
                da = fetch_direct_s3(
                    var_key, experiment, time_slice,
                    lat_bounds, lon_bounds, catalog,
                )
                da = preprocess(da, var_key)
                results[(var_key, scenario)] = da

    elif backend == "climakitae":
        # Sequential only — get_data() is NOT thread-safe
        for scenario in scenarios:
            for var_key in variables:
                da = fetch_climakitae(
                    var_key, scenario, time_slice,
                    lat_bounds, lon_bounds,
                )
                da = preprocess(da, var_key)
                results[(var_key, scenario)] = da

    elif backend == "coiled":
        dask = _get_dask()
        wkt_list, crs_str = boundary_to_wkt(boundary)

        # Build all delayed tasks
        delayed_tasks = {}
        for scenario in scenarios:
            experiment = SCENARIO_MAP.get(scenario, scenario)
            for var_key in variables:
                var_id = VARIABLE_MAP[var_key]["id"]
                paths = catalog.s3_paths(var_id, experiment)
                task = build_coiled_task(
                    var_key, var_id, paths, time_slice,
                    lat_bounds, lon_bounds, wkt_list, crs_str,
                )
                delayed_tasks[(var_key, scenario)] = task

        # Submit all at once — Dask distributes across workers
        keys = list(delayed_tasks.keys())
        computed = dask.compute(*[delayed_tasks[k] for k in keys])

        # Collect DataFrames by variable, concatenating scenarios together
        var_dfs = {var_key: [] for var_key in variables}
        for key, df in zip(keys, computed):
            var_key, scenario = key
            # Strip timing metadata columns
            timing_cols = [c for c in df.columns if c.startswith("_")]
            df = df.drop(columns=timing_cols, errors="ignore")
            # Add scenario and timescale columns so DataFrame is self-describing
            df["scenario"] = scenario
            df["timescale"] = timescale
            var_dfs[var_key].append(df)

        # Concatenate all scenarios for each variable into one DataFrame
        for var_key in variables:
            results[var_key] = pd.concat(var_dfs[var_key], ignore_index=True)

    else:
        raise ValueError(f"Unknown backend: {backend!r}")

    return results


def run_full_pipeline(
    variables: list,
    scenarios: list,
    boundary: gpd.GeoDataFrame,
    region_name: str,
    output_csv: str,
    baseline: tuple = DEFAULT_BASELINE,
    smoothing_window: int = DEFAULT_SMOOTHING_WINDOW,
    time_slice: tuple = DEFAULT_TIMESPAN,
    backend: str = "direct_s3",
    coiled_cluster=None,
    catalog: Optional[CatalogExplorer] = None,
) -> pd.DataFrame:
    """End-to-end pipeline: fetch -> preprocess -> spatial avg -> anomalies -> CSV.

    Supports incremental checkpointing — if output_csv already has results
    for some (region, scenario, variable) combos, those are skipped.

    Args:
        variables: List of output variable names (e.g. ["T_Avg", "Precip"]).
                   If "T_Avg" is in the list, "T_Max" and "T_Min" are fetched
                   automatically and averaged.
        scenarios: Friendly scenario names
        boundary: GeoDataFrame with study area
        region_name: Label for CSV output (e.g. "JoshuaTree")
        output_csv: Path to output CSV file
        baseline: Baseline period for anomalies
        smoothing_window: Rolling mean window size
        time_slice: (start_year, end_year)
        backend: "direct_s3" | "climakitae" | "coiled"
        coiled_cluster: Required if backend="coiled"
        catalog: Optional CatalogExplorer

    Returns:
        Combined DataFrame of all results
    """
    import time as _time

    # Determine which raw variables we need to fetch
    fetch_vars = set()
    for v in variables:
        if v == "T_Avg":
            fetch_vars.add("T_Max")
            fetch_vars.add("T_Min")
        else:
            fetch_vars.add(v)
    fetch_vars = sorted(fetch_vars)

    # Load checkpoint
    processed_units = load_checkpoint(output_csv)

    # Build work manifest
    all_work = [
        (region_name, sc, vk)
        for sc in scenarios
        for vk in variables
    ]
    remaining = [w for w in all_work if w not in processed_units]

    if not remaining:
        print(f"All {len(all_work)} units already done.")
        if os.path.exists(output_csv):
            return pd.read_csv(output_csv)
        return pd.DataFrame(columns=CSV_COLUMNS)

    print(f"Work units: {len(remaining)} remaining / {len(all_work)} total")

    lat_bounds, lon_bounds = get_lat_lon_bounds(boundary)
    if catalog is None and backend in ("direct_s3", "coiled"):
        catalog = CatalogExplorer()

    run_start = _time.perf_counter()
    completed = 0

    # Determine which scenarios we still need
    needed_scenarios = set()
    for (rn, sc, vk) in remaining:
        needed_scenarios.add(sc)

    # ---- Fetch data by backend ----
    if backend in ("direct_s3", "climakitae"):
        # Fetch all raw data
        raw_data = {}  # scenario -> {var_key: DataArray}
        for scenario in needed_scenarios:
            raw_data[scenario] = {}
            experiment = SCENARIO_MAP.get(scenario, scenario)
            for var_key in fetch_vars:
                print(f"  Fetching {scenario} / {var_key}...")
                t0 = _time.perf_counter()
                try:
                    if backend == "direct_s3":
                        da = fetch_direct_s3(
                            var_key, experiment, time_slice,
                            lat_bounds, lon_bounds, catalog,
                        )
                    else:
                        da = fetch_climakitae(
                            var_key, scenario, time_slice,
                            lat_bounds, lon_bounds,
                        )
                    da = preprocess(da, var_key)
                    raw_data[scenario][var_key] = da
                    print(f"    OK ({_time.perf_counter()-t0:.0f}s)")
                except Exception as e:
                    print(f"    ERROR: {e}")

        # Compute T_Avg if needed
        if "T_Avg" in variables:
            for scenario in needed_scenarios:
                raw = raw_data.get(scenario, {})
                if raw.get("T_Max") is not None and raw.get("T_Min") is not None:
                    raw["T_Avg"] = compute_t_avg(raw["T_Max"], raw["T_Min"])

        # Spatial average + anomalies + save
        for scenario in scenarios:
            raw = raw_data.get(scenario, {})
            for var_key in variables:
                if (region_name, scenario, var_key) in processed_units:
                    continue
                if raw.get(var_key) is None:
                    continue

                elapsed = (_time.perf_counter() - run_start) / 60
                completed += 1
                print(
                    f"  [{completed}/{len(remaining)}] "
                    f"{region_name} | {scenario} | {var_key} "
                    f"({elapsed:.1f} min)"
                )

                avg = spatial_average(raw[var_key], boundary)
                if avg is None:
                    print(f"    FAILED (no data after spatial avg)")
                    continue

                annual = annual_aggregate(avg, var_key)
                anomalies = compute_anomalies(annual, var_key, baseline)
                smoothed = smooth(anomalies, smoothing_window)

                df = anomaly_to_dataframe(
                    smoothed, var_key, scenario, region_name
                )
                if df is not None and len(df) > 0:
                    append_results(output_csv, df)
                    processed_units.add((region_name, scenario, var_key))
                    print(f"    SAVED ({len(df)} rows)")

    elif backend == "coiled":
        # With Coiled, each worker does fetch + process end-to-end
        # We build delayed tasks for each (scenario, var_key) raw fetch
        dask = _get_dask()
        wkt_list, crs_str = boundary_to_wkt(boundary)

        # Build tasks for raw variables
        delayed_raw = {}
        for scenario in needed_scenarios:
            experiment = SCENARIO_MAP.get(scenario, scenario)
            for var_key in fetch_vars:
                var_id = VARIABLE_MAP[var_key]["id"]
                paths = catalog.s3_paths(var_id, experiment)
                task = build_coiled_task(
                    var_key, var_id, paths, time_slice,
                    lat_bounds, lon_bounds, wkt_list, crs_str,
                )
                delayed_raw[(var_key, scenario)] = task

        # Submit all
        keys = list(delayed_raw.keys())
        print(f"  Submitting {len(keys)} tasks to Coiled workers...")
        computed = dask.compute(*[delayed_raw[k] for k in keys])

        # Organize results
        raw_dfs = {}
        for key, df in zip(keys, computed):
            timing_cols = [c for c in df.columns if c.startswith("_")]
            raw_dfs[key] = df.drop(columns=timing_cols, errors="ignore")

        # For each output variable, compute anomalies locally
        # (Coiled returns spatially-averaged monthly DataFrames)
        for scenario in scenarios:
            for var_key in variables:
                if (region_name, scenario, var_key) in processed_units:
                    continue

                completed += 1
                print(
                    f"  [{completed}/{len(remaining)}] "
                    f"{region_name} | {scenario} | {var_key} (post-processing)"
                )

                if var_key == "T_Avg":
                    tmax_df = raw_dfs.get(("T_Max", scenario))
                    tmin_df = raw_dfs.get(("T_Min", scenario))
                    if tmax_df is None or tmin_df is None:
                        print(f"    SKIP: missing T_Max or T_Min")
                        continue
                    # Average the two
                    merged = tmax_df.merge(
                        tmin_df, on=["simulation", "time"],
                        suffixes=("_max", "_min")
                    )
                    merged["T_Avg"] = (merged["T_Max"] + merged["T_Min"]) / 2
                    # Convert to xr for anomaly calc
                    avg_da = _df_to_xr(merged, "T_Avg")
                else:
                    df = raw_dfs.get((var_key, scenario))
                    if df is None:
                        print(f"    SKIP: no data")
                        continue
                    avg_da = _df_to_xr(df, var_key)

                annual = annual_aggregate(avg_da, var_key)
                anomalies = compute_anomalies(annual, var_key, baseline)
                smoothed = smooth(anomalies, smoothing_window)

                out_df = anomaly_to_dataframe(
                    smoothed, var_key, scenario, region_name
                )
                if out_df is not None and len(out_df) > 0:
                    append_results(output_csv, out_df)
                    processed_units.add((region_name, scenario, var_key))
                    print(f"    SAVED ({len(out_df)} rows)")

    total_time = _time.perf_counter() - run_start
    print(f"\nDone: {completed} units in {total_time/60:.1f} min")
    print(f"Output: {output_csv}")

    if os.path.exists(output_csv):
        return pd.read_csv(output_csv)
    return pd.DataFrame(columns=CSV_COLUMNS)


def _df_to_xr(df: pd.DataFrame, value_col: str) -> xr.DataArray:
    """Convert a Coiled worker's output DataFrame back to xarray for anomaly calc.

    The DataFrame has columns: simulation, time, <value_col>

    Returns:
        DataArray with dims (time, simulation)
    """
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    pivot = df.pivot(index="time", columns="simulation", values=value_col)
    da = xr.DataArray(
        pivot.values,
        dims=["time", "simulation"],
        coords={
            "time": pivot.index.values,
            "simulation": pivot.columns.values,
        },
    )
    return da


# ===========================================================================
# Section 10: CSV I/O & Checkpointing
# ===========================================================================

def load_checkpoint(csv_path: str) -> set:
    """Load previously processed (region, scenario, variable) tuples.

    Args:
        csv_path: Path to output CSV

    Returns:
        Set of (region, data_scenario, variable) tuples already processed
    """
    if not os.path.exists(csv_path):
        # Create empty CSV with headers
        pd.DataFrame(columns=CSV_COLUMNS).to_csv(csv_path, index=False)
        return set()

    try:
        df = pd.read_csv(csv_path, low_memory=False)
        if {"Region", "DataScenario", "Variable"}.issubset(df.columns):
            return set(
                zip(df["Region"], df["DataScenario"], df["Variable"])
            )
        return set()
    except (pd.errors.EmptyDataError, Exception):
        pd.DataFrame(columns=CSV_COLUMNS).to_csv(csv_path, index=False)
        return set()


def append_results(csv_path: str, df: pd.DataFrame):
    """Append results to CSV (incremental save).

    Creates the CSV with headers if it doesn't exist.

    Args:
        csv_path: Path to output CSV
        df: DataFrame to append
    """
    write_header = not os.path.exists(csv_path)
    cols = [c for c in CSV_COLUMNS if c in df.columns]
    df[cols].to_csv(csv_path, mode="a", header=write_header, index=False)


def anomaly_to_dataframe(anomaly_da: xr.DataArray, var_key: str,
                         scenario: str,
                         region_name: str) -> Optional[pd.DataFrame]:
    """Convert anomaly DataArray to standardized DataFrame for CSV.

    Args:
        anomaly_da: Smoothed anomaly DataArray with (time, simulation) dims
        var_key: Variable short name
        scenario: Friendly scenario name
        region_name: Region label

    Returns:
        DataFrame with columns matching CSV_COLUMNS, or None if empty
    """
    sdf = anomaly_da.to_dataframe(name="Anomaly").reset_index()
    sdf["Variable"] = var_key
    sdf["Year"] = sdf["time"].dt.year
    sdf["Region"] = region_name
    sdf["DataScenario"] = scenario

    # Label historical portion of SSP scenarios
    if scenario == "Historical Climate":
        sdf["Scenario"] = "Historical Climate"
    else:
        sdf["Scenario"] = np.where(
            sdf["Year"] <= HISTORICAL_END_YEAR,
            "Historical Climate",
            scenario,
        )

    # Normalize simulation column name
    if "simulation" in sdf.columns:
        sdf = sdf.rename(columns={"simulation": "Simulation"})
    elif "source_id" in sdf.columns:
        sdf = sdf.rename(columns={"source_id": "Simulation"})

    sdf = sdf.dropna(subset=["Anomaly"])

    cols = [c for c in CSV_COLUMNS if c in sdf.columns]
    return sdf[cols] if len(sdf) > 0 else None
