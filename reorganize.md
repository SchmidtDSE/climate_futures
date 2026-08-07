# Splitting up `ClimateFutures`: saving results, retiring the Python plots, and running many parks

## Context

Right now `src/climateFutures.py` has one class, `ClimateFutures`, that does three unrelated jobs at once: it computes anomalies and the climate-future classification, it makes matplotlib/plotnine plots (including a ~130-line manual legend-building block in `plot_quadrants`), and it's only ever built for one park at a time. That's why it feels like it's "distracting from what the class is actually doing" — the plotting code is roughly two-thirds of the file's line count, and it's mixed directly into the same object as the science.

You want three things:
1. Save the `classify()` table (and the numbers needed to redraw its plot) to disk instead of only living in memory.
2. Stop maintaining the matplotlib/plotnine plotting code as the "real" diagnostics — hand that job to native `ggplot2` in R/Quarto instead — but keep the old methods around for now rather than deleting them outright.
3. Do all of this in a way that works cleanly once you're running the same classification across a handful of parks, not just one.

You also asked for this translated away from software-engineering jargon. The one idea worth naming plainly, because it drives every decision below, is: **a class (or a script) should have one job.** Right now `ClimateFutures` has three jobs (compute, plot, single-park-only). The plan below gives each job its own home: computing stays in `ClimateFutures`, saving gets three tiny functions in a new file, running-many-parks gets one small loop, and plotting moves to a Quarto report — the same way you wouldn't want the script that regrids and computes anomalies to also contain all your figure-formatting code; you'd keep the calculation reusable and let something else make the figures from its saved output.

## What changes, and why

### 1. Saving results: three plain CSV files, not one

Recommendation: **CSV, not Parquet/Feather.** Your results table is small (at most a few hundred rows even with a dozen parks) — the compression/speed advantages of a binary format don't matter at this size, but CSV's readability does: `git diff` on a CSV shows you exactly which park's classification flipped and why, which is real debugging value for a number that can shift when you rerun with a different model list. It also needs zero new dependencies on either the Python or R side (pandas and R's `readr` both read CSV natively).

Three files, each covering **all parks together** (not one file per park), because your Quarto report reads once and facets by park:

- `results/classification_all_parks.csv` — the `classify()` output, columns: `park, model, scenario, tas, pr, climate_future`, plus a few small provenance columns (`baseline_start, baseline_end, data_source, run_timestamp`) so a row is traceable without opening another file.
- `results/classification_thresholds_all_parks.csv` — the 25th/50th/75th percentile thresholds that `classify()` already computes internally to decide `climate_future`, but currently throws away. Long format: `park, variable, quantile, value`. This is what lets the R report redraw the quadrant box and median lines **without recomputing them** — R just reads the numbers Python already worked out.
- `results/run_metadata_{park}.json` — one small file *per park* (not combined): the model list, scenario list, baseline period, and which model/scenario combos were skipped (missing data), for that run. This is just a paper trail; the plot doesn't need it.

**Important housekeeping catch:** your `.gitignore` currently has a blanket `*.csv` rule (line 10, labeled "Test point csv"). Any file under `results/` will be silently ignored by git unless we add a negation line. I'll add:
```
!results/*.csv
```
right after the existing `*.csv` line. Otherwise you'll save a file, `git status` will show nothing, and it'll look like saving silently did nothing.

**Re-running one park shouldn't clobber the others.** The save function for the two combined CSVs will: read the existing file if present, drop any rows already belonging to *this* park, add the new rows for this park, then write the whole thing back. So rerunning `jotr` only ever touches `jotr`'s rows.

### 2. Where the saving code lives: a new `src/results.py`, not new methods on `ClimateFutures`

This is the "one job per thing" idea again: `ClimateFutures` already has a full job (load data, compute anomalies, classify). Whether the result then goes to a CSV, a database, or a cloud bucket next year is a separate concern — if you ever swap the storage format, you shouldn't have to touch the classification math to do it, and vice versa.

At the same time, this doesn't need to become a big abstraction — there's one format, one place, one user (you). So `src/results.py` is just **three small functions**, nothing fancier:
```python
save_classification(df, results_dir="results")
save_thresholds(thresholds_df, park, results_dir="results")
save_run_metadata(park, models, scenarios, baseline_period, data_source, skipped, results_dir="results")
```

**One small, non-breaking addition to `classify()`** (`src/climateFutures.py:135-183`): right now the quantile thresholds and the list of skipped model/scenario combos are computed inside `classify()` but only exist as local variables that vanish when the function returns. I'll add two lines right where they're already computed, storing them as `self.thresholds_` and `self.skipped_` (the trailing-underscore naming is the same convention scikit-learn uses for "here's something this call computed and stashed on the object"). `classify()` still *returns* exactly the same dataframe as before, so nothing that already calls `classify()` — including the plotting methods — needs to change.

### 3. Running many parks: one small loop, config values in the file already meant for them

New file: `src/runClassification.py`, with one function that loops over a list of parks, builds a `ClimateFutures` for each, classifies it, and saves all three outputs via `results.py`:
```python
def run_classification(parks, models, scenarios, baseline_period, load_fn=None, results_dir="results"):
    for park in parks:
        cf = ClimateFutures(models, scenarios, park, baseline_period, load_fn=load_fn)
        df = cf.classify()
        save_classification(df, results_dir)
        save_thresholds(cf.thresholds_, park, results_dir)
        save_run_metadata(park, models, scenarios, baseline_period, cf.load_fn.__name__, cf.skipped_, results_dir)
    ...
```
This directly replaces the pattern where the 15-model CalAdapt list is hand-typed inline in `climateFutures.ipynb` for a single park — the same copy-paste-per-park problem your bash scripts (`isimip/process_batch_jotr.sh` vs `process_batch_mojave.sh`) already have, just one level up, in Python. This plan doesn't touch the bash scripts (that's a separate, already-tracked TODO in `plan_oberhaul.md`), but it stops the same pattern from spreading into the Python side too.

The park list and shared run settings (models, scenarios, baseline period) get a home in `src/configs.py` — which is currently an empty one-line stub, even though the notebook's own intro markdown already claims "`configs.py` defines global variables." Plain constants, nothing more:
```python
PARKS = ["jotr", "deva", "moja"]
BASELINE_PERIOD = ("1950", "2014")
SCENARIOS = ["ssp370", "ssp585", "historical"]
MODELS_CALADAPT = [...]  # the 15 models, moved out of the notebook
```
No YAML file, no config-management library — this is a handful of values you'll hand-edit occasionally, and plain Python constants give you import-checking and autocomplete for free, which a YAML file wouldn't.

The relevant cell in `climateFutures.ipynb` becomes:
```python
from src.runClassification import run_classification
from src.configs import PARKS, MODELS_CALADAPT, SCENARIOS, BASELINE_PERIOD
from src.dataLoader import DataLoader

combined_df = run_classification(PARKS, MODELS_CALADAPT, SCENARIOS, BASELINE_PERIOD,
                                  load_fn=DataLoader().load_caladapt)
```

### 4. The Quarto report: one file, reads the CSVs, facets by park

New file: `reports/climate_futures_report.qmd`. Per your choice, this is **one combined report that facets by park** (not one report per park), and it's rendered **manually** by you, on demand — Python's job stops at saving the CSVs; nothing calls Quarto automatically.

Structure (chunk-level plan, not full R code):
1. **YAML header** — self-contained HTML output (`embed-resources: true`) so it's one file you can open without a server, table of contents on, R code hidden by default (`echo: false`) since this is a plots report, not a code walkthrough.
2. **Setup chunk** — loads `readr`, `dplyr`, `tidyr`, `ggplot2` (all part of the single `tidyverse` package), reads both `results/classification_all_parks.csv` and `results/classification_thresholds_all_parks.csv`, and pivots the thresholds table wide (one row per park, columns like `tas_0.25, tas_0.75, pr_0.25, pr_0.75`) so it's ready to hand to `geom_rect()`.
3. **Quadrant plot chunk** — the direct replacement for `plot_quadrants()`: `geom_rect()` + `geom_vline()`/`geom_hline()` from the thresholds table, `geom_point()` from the results table colored by `climate_future` and shaped by `model`, then `facet_wrap(~park, scales = "free")`. This is the standard ggplot2 trick of feeding one layer a smaller reference dataframe than the rest of the plot, tied together by the shared `park` column in the facet — it draws the right box in the right panel automatically, and it means the hand-built matplotlib legend code disappears entirely, since ggplot2's own legends combine color/shape without help.
4. **Left for a later, deliberate decision, not built now:** reproducing the timeseries plots (`plot_ensemble`/`plot_climate_futures`) in R would need a genuinely different export — a full monthly anomaly time series per model/scenario/park, which nothing saves today (only the single mid-century number per combo is computed). I'm flagging this rather than quietly building it, since it's extra scope beyond "save the classify() table."

### 5. Keeping the old plots, but visibly marked as on their way out

Per your call to keep `plot_timeseries`, `plot_ensemble`, `plot_climate_futures`, and `plot_quadrants` rather than delete them: each gets a `warnings.warn(..., DeprecationWarning)` as the first line of its body, plus a one-line docstring note pointing at `reports/climate_futures_report.qmd` as the replacement. One technical wrinkle worth knowing: Python normally *suppresses* `DeprecationWarning`s raised from inside an imported module (as opposed to code run directly), so the warning could go unnoticed in a notebook unless the notebook itself opts in. I'll add one line near the top of `climateFutures.ipynb` — `warnings.simplefilter("always", DeprecationWarning)` — so it reliably shows up there, without the library itself changing global behavior for anyone else who imports it.

## Files touched

- `src/climateFutures.py` — add `import warnings`; stash `self.thresholds_`/`self.skipped_` in `classify()`; add a deprecation warning + docstring note to each `plot_*` method (no behavior changes otherwise).
- `src/results.py` (new) — `save_classification`, `save_thresholds`, `save_run_metadata`.
- `src/runClassification.py` (new) — `run_classification(...)` loop.
- `src/configs.py` — fill in `PARKS`, `BASELINE_PERIOD`, `SCENARIOS`, `MODELS_CALADAPT`.
- `reports/climate_futures_report.qmd` (new) — the combined, park-faceted ggplot2 report.
- `.gitignore` — add `!results/*.csv` so results actually get committed.
- `climateFutures.ipynb` — replace the single-park hardcoded cell with the `run_classification(...)` call; add the `warnings.simplefilter` line.

**Not touched, out of scope:** `isimip/process_batch_*.sh` (separate, already-tracked TODO), the devcontainer/environment (you already decided to work locally for now), timeseries-plot replacement in R (flagged above as a later decision).

## Prerequisites this creates (nothing to install now, just flagging)

- Python side: none — CSV read/write needs only pandas, already present.
- R side (entirely outside conda, on your locally-installed R/Quarto): the `tidyverse` package (`readr`, `dplyr`, `tidyr`, `ggplot2`) and `knitr` for Quarto to run R chunks. These aren't tracked by `conda-lock` today, since R lives outside the conda environment — that's fine for a single-user local workflow, just not forgotten.

## Verification

1. Run `run_classification(...)` for at least two parks (e.g. `jotr` and `deva`) from the notebook; confirm `results/classification_all_parks.csv` contains rows for both, `results/classification_thresholds_all_parks.csv` has threshold rows for both, and one `run_metadata_{park}.json` exists per park.
2. Rerun for just `jotr` again with a shortened model list; confirm `deva`'s rows in both combined CSVs are untouched and `jotr`'s rows reflect only the new run (no duplicates).
3. Run `git status`; confirm the new `results/*.csv` files show up as untracked/changed (proves the `.gitignore` fix works) rather than being silently ignored.
4. Call `cf.plot_quadrants()` once from the notebook; confirm the `DeprecationWarning` is visible in the notebook output.
5. Open `reports/climate_futures_report.qmd` in Quarto/RStudio and render it; confirm the quadrant plot shows one facet per park, with each park's reference box/median lines in the right place, and that it visually matches what `plot_quadrants()` currently produces per park.