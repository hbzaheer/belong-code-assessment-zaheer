import os
import pandas as pd
from assessment.stats import *

df = pd.read_json(f'{os.getcwd()}/tests/test_sample_data.json')

def test_top_n_by_day():

    # random sample of the pedestrian dataset for testing
    top_n = get_top_n_records_by_grain(df, 'day', 'sensor_name', 'hourly_counts', 5)

    # check the 5th result for Friday
    test_1 = top_n.loc[(top_n["day"] == "Friday") & (top_n["rank"] == 5)].iloc[0]
    assert test_1["sensor_name"] == "Flinders Street Station Underpass"

    # check the 2nd result for Sunday
    test_2 = top_n.loc[(top_n["day"] == "Sunday") & (top_n["rank"] == 2)].iloc[0]
    assert test_2["sensor_name"] == "New Quay"

    # check the 1st result for Saturday
    test_3 = top_n.loc[(top_n["day"] == "Saturday") & (top_n["rank"] == 1)].iloc[0]
    assert test_3["sensor_name"] == "Flinders St-Elizabeth St (East)"


def test_top_n_by_month():

    # this data contains only values for September and November
    top_n = get_top_n_records_by_grain(df, "month", 'sensor_name', 'hourly_counts', 5)

    # check the 1st result for September
    test_1 = top_n.loc[(top_n["month"] == "September") & (top_n["rank"] == 1)].iloc[0]
    assert test_1["sensor_name"] == "Birrarung Marr"

    # check the 5th result for November
    test_2 = top_n.loc[(top_n["month"] == "November") & (top_n["rank"] == 5)].iloc[0]
    assert test_2["sensor_name"] == "Princes Bridge"

    