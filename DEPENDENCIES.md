# Updating Dependencies

## Current Setup

This project uses conda-lock to manage dependencies across different platforms. The lock file ensures reproducible environments.

## Quick Start - Installing Cartopy (Already Done)

Cartopy has been installed in the current environment:
```bash
conda install -y -c conda-forge cartopy
```

## Regenerating the Lock File (For Future Updates)

When you need to update dependencies or add new packages:

### 1. Edit `environment.yml`
Add or update packages in the `environment.yml` file.

### 2. Generate a new lock file
```bash
# Install conda-lock if not already installed
conda install -y conda-lock

# Generate the lock file for Linux (current platform)
conda-lock lock -f environment.yml -p linux-64

# This will update conda-linux-64.lock
```

### 3. Rebuild the dev container
After updating the lock file, rebuild the dev container to use the new dependencies:
- In VS Code: Open Command Palette (Ctrl+Shift+P) → "Dev Containers: Rebuild Container"
- Or rebuild using Docker manually

## Current Dependencies

The environment includes:
- **Core**: Python 3.12, numpy, scipy, pandas, xarray
- **Geospatial**: geopandas, rasterio, shapely, pyogrio, **cartopy** (for mapping)
- **Visualization**: matplotlib, bokeh, folium, ipywidgets
- **Climate**: cftime, cf_xarray, pint-xarray, xclim, xmip
- **Data Access**: intake, s3fs, boto3
- **Jupyter**: ipykernel, jupyterlab

## Notes

- The lock file (`conda-linux-64.lock`) is platform-specific (linux-64)
- If working on other platforms, generate additional lock files with `-p osx-64` or `-p win-64`
- The Dockerfile installs packages from the lock file to ensure reproducibility
