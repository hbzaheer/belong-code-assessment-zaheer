from assessment.helper_scripts import is_columns_exists, is_contains_data

def get_top_n_records_by_grain(df, time_column: str, query_column: str, sum_column: str, top_n: int):
    """
    Returns the top n records by time grain.
    params df: pandas dataframe as read from landed source data file.
    params time_column: column name for time granularity, e.g. day, month etc.
    params query_column: column name for sensor locations.
    params sum_column: column name for sum of hourly pedestrian counts.
    params top_n: number of top records to return.
    returns: pandas dataframe with top n records by time grain.
    """
    # Check if the columns exist in the dataframe.
    if not is_columns_exists(df, [time_column, query_column, sum_column]):
        return None

    # Check if the dataframe contains any data.
    if not is_contains_data(df):
        return None

    # Get sum of pedestrian counts for each sensor per unique value of the time grain
    top_n_df = df.groupby([time_column, query_column])[sum_column].sum().reset_index().sort_values(by=[time_column, sum_column], ascending=False)

    # Rank the pedestrian counts in descending order for each unique value of the time grain
    top_n_df['rank'] = top_n_df.groupby(time_column)[sum_column].rank(method='first', ascending=False).astype(int)

    # Get the top 10 rows for each unique value of the time grain
    top_n_df = top_n_df.reset_index(drop=True).groupby(time_column).head(top_n)
    
    return top_n_df


def most_growth_in_past_year(df, date_column: str, time_column: str, query_column: str, sum_column: str):
    """
    Returns record of the Sensor Location with the highest growth in the past year.
    params df: pandas dataframe as read from landed source data file.
    params date_column: records timestamp column in the dataframe.
    params time_column: time grain column in the dataframe.
    params query_column: column name for sensor locations.
    params sum_column: column name for sum of hourly pedestrian counts.
    returns: record of the sensor location with the highest growth in the past year.
    """
    # Check if the columns exist in the dataframe.
    if not is_columns_exists(df, [date_column, time_column, query_column, sum_column]):
        return None

    # Check if the dataframe contains any data.
    if not is_contains_data(df):
        return None

    # Add helper columns for calculating growth in past year
    df['month_year'] = df[date_column].dt.strftime('%Y-%m')
    df['month_no'] = df[date_column].dt.strftime('%-m')

    # Calculate monthly totals
    df_sum = df.groupby([query_column, time_column, 'month_no', 'month_year'])[sum_column].sum().reset_index().sort_values(by=[query_column, 'month_year'])

    # Calculate rolling sum for each sensor for past 12 months
    df_sum['rolling_sum'] = df_sum.set_index('month_year').groupby([query_column])[sum_column].rolling(window=12).sum().fillna(0).reset_index()[sum_column]
    
    # Get the last month for which data is available
    latest_month = df[date_column].max().strftime('%-m')
    latest_year = df[date_column].max().strftime('%Y')
    
    # Filter for only last 24 months
    df_yearly = df_sum.loc[(df_sum[time_column] >= int(latest_year)-1) & (df_sum['month_no'] == latest_month) & (df_sum['rolling_sum'] > 0)]
    
    # Get past years sum of hourly counts for each sensor
    df_yearly['past_rolling_sum'] = df_yearly.groupby([query_column])['rolling_sum'].shift(1).fillna(0)

    # Calculate growth in past year
    df_yearly['growth'] = df_yearly['rolling_sum'] - df_yearly['past_rolling_sum']
    
    df_yearly = df_yearly[(df_yearly[time_column] == int(latest_year)) & (df_yearly['past_rolling_sum'] > 0)].sort_values(by=['growth'], ascending=False)

    return df_yearly[[query_column, 'growth']].head(1)


