#climateFutures
import pandas as pd
import numpy as np

import xarray as xr
import geopandas as gpd
import rioxarray as rxr
from shapely.geometry import mapping
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.lines import Line2D
from plotnine import (ggplot, aes, geom_point, geom_vline, geom_hline,
                      geom_rect, scale_fill_manual, scale_color_manual,
                      scale_shape_manual, labs, theme_bw, theme)

from src import dataLoader
from src import config

class ClimateFutures:
    ''' This class contains all the functions to create climate future datasets'''

    def __init__(self, models, scenarios, park, baseline_period, load_fn=None):
        self.loader = dataLoader.DataLoader()
        self.models = models
        self.scenarios = scenarios
        self.park = park
        self.boundary = gpd.read_file(f'boundaries/{park}.shp')
        self.baseline_period = baseline_period
        self.load_fn = load_fn if load_fn is not None else self.loader.load_isimip

    def calculate_anomaly(self, scenario, baseline_period, model, variable, boundary):
        ''' Takes a clipped xarray object and calculates the anomaly relative to the provided baseline period. 
        Returns an xarray object of the anomaly. '''

        nc = self.load_fn(scenario, model, variable, boundary, self.park)
        nc_hist = self.load_fn("historical", model, variable, boundary, self.park)
        baseline = nc_hist.sel(time=slice(baseline_period[0], baseline_period[1])).mean("time")
        anomaly = nc - baseline

        return(anomaly)


    def mid_century_anomalies(self, scenario, baseline_period, model, variable, boundary):
        anomaly = self.calculate_anomaly(scenario, baseline_period, model, variable, boundary)
        mid_century_anomaly = anomaly.sel(time=slice("2035", "2065")).mean("time")

        return(mid_century_anomaly)
    

    def classify(self):
            all_data = []
            skipped = []
            for model in self.models:
                for scenario in self.scenarios:
                    if scenario == 'historical':
                        continue
                    try:
                        anomaly_tas = self.mid_century_anomalies(scenario, self.baseline_period, model, 'tas', self.boundary)
                        anomaly_pr = self.mid_century_anomalies(scenario, self.baseline_period, model, 'pr', self.boundary)
                    except FileNotFoundError:
                        skipped.append((model, scenario))
                        continue
                    data = {
                        'model': model,
                        'scenario': scenario,
                        'park': self.park,
                        'tas': anomaly_tas.item(),
                        'pr': anomaly_pr.item()
                    }
                    all_data.append(data)
    
            if skipped:
                print(f"Skipped {len(skipped)} missing model/scenario combinations: {skipped}")
    
            df = pd.DataFrame(all_data)
    
            quantiles = df[['tas', 'pr']].quantile([0.25, 0.5, 0.75])

            quantiles.to_csv(f'{config.OUTPUT}/climate_futures_quantiles_{self.park}.csv', index=True)
    
            conditions = [
                # warm-dry
                ((df['tas'] < quantiles.loc[0.25, 'tas']) & (df['pr'] < quantiles.loc[0.50, 'pr'])) |
                ((df['tas'] < quantiles.loc[0.50, 'tas']) & (df['pr'] < quantiles.loc[0.25, 'pr'])),
                # warm-wet
                ((df['tas'] < quantiles.loc[0.25, 'tas']) & (df['pr'] > quantiles.loc[0.50, 'pr'])) |
                ((df['tas'] < quantiles.loc[0.50, 'tas']) & (df['pr'] > quantiles.loc[0.75, 'pr'])),
                # hot-dry
                ((df['tas'] > quantiles.loc[0.75, 'tas']) & (df['pr'] < quantiles.loc[0.50, 'pr'])) |
                ((df['tas'] > quantiles.loc[0.50, 'tas']) & (df['pr'] < quantiles.loc[0.25, 'pr'])),
                # hot-wet
                ((df['tas'] > quantiles.loc[0.75, 'tas']) & (df['pr'] > quantiles.loc[0.50, 'pr'])) |
                ((df['tas'] > quantiles.loc[0.50, 'tas']) & (df['pr'] > quantiles.loc[0.75, 'pr']))
            ]
    
            future = ['warm-dry', 'warm-wet', 'hot-dry', 'hot-wet']
    
            df['climate_future'] = np.select(conditions, future, default='central')
    
            return df

    def create_ensemble(self, variable, futures=['warm-dry', 'warm-wet', 'hot-dry', 'hot-wet']):
        ''' Based on classification, creaste aggregateddata over the study area to plot 
        time seriesensemble of climate futures'''
        print(self)
        
        df = self.classify()
        #Filter for the climate futures we want.
        df = df[df['climate_future'].isin(futures)] if futures is not None else df
    
        all_data = []

        # getting historic data for all models and attach attributes
        for model in df['model'].unique():
            nc_hist = self.load_fn("historical", model, variable, self.boundary, self.park)
            all_data.append(nc_hist.expand_dims('member').assign_coords(
                model=('member', [model]),
                scenario=('member', ['historical']),
                climate_future=('member', ['historical']),
            ))

            # getting data for all model/scenario comibinations that actually exist  and attach attributes
            for row in df[df['model'] == model].itertuples():
                nc = self.load_fn(row.scenario, row.model, variable, self.boundary, self.park)
                all_data.append(nc.expand_dims('member').assign_coords(
                    model=('member', [row.model]),
                    scenario=('member', [row.scenario]),
                    climate_future=('member', [row.climate_future]),
                ))
                print(f"Added {row.model} {row.scenario} ({row.climate_future}) to ensemble")

        ensemble = xr.concat(all_data, dim='member')

        result = ensemble.to_dataframe().reset_index()
        result = result.drop(columns=['spatial_ref', 'member'])
        result.to_csv(f'{config.OUTPUT}/ensemble_{variable}_{self.park}.csv', index=False)

        return ensemble

    ### Old plotting functions, can be used for diagnostics but not actively maintained

    def plot_timeseries(self, ax, scenario, model, baseline_period, boundary, variable, color=None):
        ''' Plot a single smoothed anomaly timeseries. color overrides the default scenario color. '''

        if color is None:
            if scenario == "historical":
                color = "#555555"  # dark grey
            elif scenario == "ssp126":
                color = "#ffd580"  # light orange
            elif scenario == "ssp370":
                color = "#ff9900"  # dark orange
            elif scenario == "ssp585":
                color = "#e95462"  # red
            else:
                color = "gray"

        anomaly = self.calculate_anomaly(scenario, baseline_period, model, variable, boundary)
        anomaly_smooth = anomaly.rolling(time=36, center=True, min_periods=1).mean()
        plot = anomaly_smooth.plot(ax=ax, label=f"{model.split('_')[0]} {scenario}", color=color, alpha=1)

        return(plot)

    def plot_ensemble(self, variable, color_map=None, include=None, xlim=None, shaded_period=None):
        ''' Plot all model/scenario timeseries.
        color_map: optional dict mapping (model, scenario) -> color.
                   If None, defaults to scenario-based colors.
        include: optional set of (model, scenario) pairs to plot.
                 If None, all model/scenario combinations are plotted.
        xlim: optional (start, end) tuple of year strings to set the x-axis range, e.g. ('1990', None).
        shaded_period: optional (start, end) tuple of year strings to draw a grey shaded band, e.g. ('2035', '2065'). '''

        fig, ax = plt.subplots(figsize=(14, 6))

        for model in self.models:
            for scenario in self.scenarios:
                if include is not None and scenario != 'historical' and (model, scenario) not in include:
                    continue
                color = color_map.get((model, scenario)) if color_map else None
                try:
                    self.plot_timeseries(ax, scenario, model, self.baseline_period,
                                         self.boundary, variable, color=color)
                except FileNotFoundError:
                    continue

        if shaded_period is not None:
            ax.axvspan(pd.Timestamp(shaded_period[0]), pd.Timestamp(shaded_period[1]),
                       color='gray', alpha=0.15, zorder=0, label='Mid-century (2035–2065)')

        if xlim is not None:
            ax.set_xlim(
                pd.Timestamp(xlim[0]) if xlim[0] else None,
                pd.Timestamp(xlim[1]) if xlim[1] else None
            )

        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.set_title(f'Anomaly (relative to 1979-2012) by Model and Scenario for {variable}')
        ax.set_ylabel('Anomaly')
        plt.tight_layout()
        plt.show()

    def plot_climate_futures(self, variable, futures=None):
        ''' Like plot_ensemble but lines are colored by climate future instead of scenario.
        futures: list of climate future labels to include, e.g. ['warm-wet', 'hot-dry'].
                 If None, all classified futures are shown. '''

        future_colors = {
            'warm-dry': '#fea973',
            'warm-wet': '#e95462',
            'hot-dry': '#331067',
            'hot-wet': '#b5367a',
            'central': 'gray'
        }

        df = self.classify()

        # Filter to only the requested futures (keep historical which has no future label)
        if futures is not None:
            df = df[df['climate_future'].isin(futures)]

        color_map = {
            (row['model'], row['scenario']): future_colors.get(row['climate_future'], 'gray')
            for _, row in df.iterrows()
        }

        # Only plot model/scenario pairs that are in the filtered set (plus historical)
        included = set(color_map.keys())
        self.plot_ensemble(variable, color_map=color_map, include=included,
                           xlim=('1990', None), shaded_period=('2035', '2065'))


    def plot_quadrants(self):
        ''' Plots a climate futures quadrant scatter plot using plotnine.
        Scenario fill style is handled via separate geom_point layers:
          0 = filled (fill=climate_future color, black border)
          1 = hollow (no fill, colored border)
          2+ = double-ring (two hollow layers at different sizes)
        Plotnine's auto-legend is suppressed; a single combined matplotlib legend is built. '''

        future_colors = {
            'warm-dry': '#fea973',
            'warm-wet': '#e95462',
            'hot-dry': '#331067',
            'hot-wet': '#b5367a',
            'central': 'gray'
        }

        marker_pool = ['o', 's', '^', 'D', 'v', 'P', 'X']

        df = self.classify()
        df['model_short'] = df['model'].str.split('_').str[0]
        model_short_list = df['model_short'].unique().tolist()
        shape_map = {m: marker_pool[i % len(marker_pool)] for i, m in enumerate(model_short_list)}

        scenarios = [s for s in self.scenarios if s != 'historical']
        quantiles = df[['tas', 'pr']].quantile([0.25, 0.5, 0.75])

        iqr_box = pd.DataFrame({
            'xmin': [quantiles.loc[0.25, 'tas']],
            'xmax': [quantiles.loc[0.75, 'tas']],
            'ymin': [quantiles.loc[0.25, 'pr']],
            'ymax': [quantiles.loc[0.75, 'pr']]
        })

        # Base plot with reference lines and IQR box
        plot = (
            ggplot(df, aes(x='tas', y='pr'))
            + geom_rect(iqr_box, aes(xmin='xmin', xmax='xmax', ymin='ymin', ymax='ymax'),
                        fill='none', color='gray', linetype='dashed', size=0.8, inherit_aes=False)
            + geom_vline(xintercept=df['tas'].median(), color='gray', linetype='dashed')
            + geom_hline(yintercept=df['pr'].median(), color='gray', linetype='dashed')
        )

        # Add one geom_point layer per scenario to control fill style
        # Points are color-coded by climate future
        for i, scenario in enumerate(scenarios):
            subset = df[df['scenario'] == scenario].copy()
            if subset.empty:
                continue
            if i == 0:
                # Filled: climate_future color fill, black border
                plot = plot + geom_point(
                    data=subset,
                    mapping=aes(x='tas', y='pr',
                                fill='climate_future', shape='model_short'),
                    color='black', size=4, stroke=0.8, inherit_aes=False
                )
            elif i == 1:
                # Hollow: no fill, colored border
                plot = plot + geom_point(
                    data=subset,
                    mapping=aes(x='tas', y='pr',
                                color='climate_future', shape='model_short'),
                    fill='none', size=4, stroke=1, inherit_aes=False
                )
            else:
                # Double-ring outer
                plot = plot + geom_point(
                    data=subset,
                    mapping=aes(x='tas', y='pr',
                                color='climate_future', shape='model_short'),
                    fill='none', size=4, stroke=0.8, inherit_aes=False
                )
                # Double-ring inner
                plot = plot + geom_point(
                    data=subset,
                    mapping=aes(x='tas', y='pr',
                                color='climate_future', shape='model_short'),
                    fill='none', size=1, stroke=0.8, inherit_aes=False
                )

        # Suppress plotnine's auto-legend; we'll build a clean matplotlib legend instead
        plot = (
            plot
            + scale_fill_manual(values=future_colors)
            + scale_color_manual(values=future_colors, guide=None)
            + scale_shape_manual(values=shape_map)
            + labs(x='Temperature Anomaly (K)', y='Precipitation Anomaly (mm/day)',
                   title=f'Climate Futures for {self.park.upper()}')
            + theme_bw()
            + theme(legend_position='none')
        )

        fig = plot.draw()
        ax = fig.axes[0]

        # Build a single combined matplotlib legend with section headers
        fill_labels = ['filled', 'hollow', 'double-ring']
        legend_handles = []

        # Climate Future section
        legend_handles.append(patches.Patch(facecolor='none', edgecolor='none',
                                            label='Climate Future'))
        for future, color in future_colors.items():
            legend_handles.append(patches.Patch(facecolor=color, edgecolor='k',
                                                label=f'  {future}'))

        # Model section
        legend_handles.append(patches.Patch(facecolor='none', edgecolor='none', label='Model'))
        for m, shape_char in shape_map.items():
            legend_handles.append(Line2D([0], [0], marker=shape_char, color='w',
                                         markerfacecolor='gray', markeredgecolor='k',
                                         markersize=8, label=f'  {m}'))

        # Scenario section
        legend_handles.append(patches.Patch(facecolor='none', edgecolor='none', label='Scenario'))
        for i, scenario in enumerate(scenarios):
            style = fill_labels[min(i, len(fill_labels) - 1)]
            fc = 'gray' if i == 0 else 'white'
            legend_handles.append(Line2D([0], [0], marker='o', color='w',
                                         markerfacecolor=fc, markeredgecolor='k',
                                         markersize=8, label=f'  {scenario} ({style})'))

        ax.legend(handles=legend_handles, loc='upper left', bbox_to_anchor=(1.02, 1),
                  fontsize=9, framealpha=1)

        return fig
