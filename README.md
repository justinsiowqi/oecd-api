# OECD API Downloader

This project provides a set of Python classes to facilitate querying, collecting, and processing macroeconomic data from OECD.

## Classes Overview

### 1. DataQuerier

The DataQuerier class fetches the coin symbol and the individual metric URLs from OECD.
```
from data_querier import DataQuerier

# Paste the OECD URLs Here
macro_path_list = [
    'https://stats.oecd.org/SDMX-JSON/data/MEI_ARCHIVE/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA17+EA18+EU15+EA19+EA20+E15+EUU+E11+EUZ+G4E+G-7+NAFTA+OEU+G20+OECDE+OECD+OEE+ONM+A5M+NMEC+ARG+BRA+CHN+IND+IDN+RUS+ZAF.107.202305.Q/all?startTime=2008-Q4&endTime=2023-Q2&dimensionAtObservation=allDimensions'
]

# Initialize 
url_updater = URLUpdater()

# Get Updated URLs with the Current Year and Month
updated_macro_path_list = url_updater.update_data(macro_path_list)
updated_macro_path_list
```

### 2. DataCollector

The DataCollector class fetches and process data from the OECD API.
```
from data_collector import DataCollector

# Initialize
collector = DataCollector()

# Downloads Individual Metrics
collector.process_data(updated_macro_path_list)
```

### 3. FileCombiner

The FileCombiner class combines multiple CSVs into a single DataFrame.
```
from file_combiner import FileCombiner

# Initialize
combiner = FileCombiner()

# Combine the CSV
combiner.join_csv('macro-indicators')
```

## Setting Up

### Install Packages
Download external libraries: simplejson, requests, pandas
```
pip install -r requirements.txt
```