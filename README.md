# Belong Coding Assessment
This repository contains the my solution to Belong's coding assessment.

# Overview
The **main.py** reads the datasets from the source api, dumps the data on the disk in json files, and extracts the below stats:
* Top 10 (most pedestrians) locations by day.
* Top 10 (most pedestrians) locations by month.
* Which location has most growth in last year

Sensor Locations data is only being dumped onto a JSON file on the disk. It's not being used anywhere in the calculations, as it's not needed for the required calculations.

# Project Directory Structure
* secrets_manager:
    * secrets.py: api access details
* tests:
    * Python test script
    * Sample data for tests (JSON)
* helper_scripts.py: Generic helper functions
* landing_configurations.json: Data landing and loading configurations
* stats.py: stats calculation functions

# Approach
## Design Decisions
### Datasets
Provided Datasets:
* [City of Melbourne Pedestrian Counting - Monthly (counts per hour)](https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-2009-to-Present-counts-/b2ak-trbp)
* [City of Melbourne Pedestrian Counting - Sensor Locations](https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234)

### Data extraction
The data is extracted from the Socrata Open Data API (SODA). Data can also be extracted by downloading CSV file. This solution only implements extraction through API in JSON format employing multithreading, which makes it considerably faster.

### Python libraries
* [pandas](https://pandas.pydata.org/)

### Testing
The calculated stats have been unit-tested using a sample datset.

# Running the code
The code has been built and tested on MacBook Pro (16-inch, 2022), on VS Code.

The main.py script can be run locally by:
1. Cloning the repository
```
git clone https://github.com/hbzaheer/belong-code-assessment-zaheer.git
```

2. Run the main script
```
main.py
```

3. Run tests (Optional)
```
./tests/test_stats.py
```