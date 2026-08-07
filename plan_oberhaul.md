# Plan to overhaul repository

- [x] Intagrate functionality to download data from `climate_data_pipeline` repository and download data for Death Valley NP (`deva`)

- [x] Debug `climate_futures_class.ipynb` and depreciate `climate_futures.ipynb` once it's working

- [ ] Rework notebook into a flat structure where functions (and classes?) are defined in scripts and sourced inside the notebook. 
    - [x] implement flat structure
    - [x] populate configs
    - [ ] organise outputs and save classification
    - [ ] add output or results class and write functions so save locally or directly to bucket
    - [ ] Move viz into native R/ Quarto
    - [ ] Nice design and color schemes

- [ ] Possibly part of above, but if we want to have it in a container, plot rendering issues need to be fixed. 

