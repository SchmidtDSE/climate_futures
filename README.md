# Climate Futures classification

This notebook executes methods to classify a climate scenario data ensemble into the National Park Service's *Climate Futures framework*.

`src` contain all the function needed:
- `configs.py` defined global variables
- `dataLoader.py` is a class with methods to load various data sets. Sofar ISIMIP 3b, and CalAdapt (CMIP6 downscaled with LOCA2-hyrbrid) have been implemented. If you are interested in using a different data source, you will need to add a method to the `dataLoader()` class
- `climateFutures.py` contains all the functions needed to classify scenarios. If your `dataLoader()` method is written correctly, it should be able to work with any newly added dataset. 

Note that our classification orientates itself from the original *Climate Futures* methodology, but does not completely reproduce it. Important differences:
- Orignal CF uses MACA-downscaled CMIP5, we use LOCA2-downscaled CMIP6 for our main applications
- Orignal CF uses observations as the baseline for anomaly calculations, we use historical simulations of the model of questions

More information on the original *Climate Futures* methodology can be found here: https://www.nps.gov/subjects/climatechange/climatefutures.htm