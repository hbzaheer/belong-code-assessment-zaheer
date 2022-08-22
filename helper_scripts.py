

def prepare_base_url(secret):
    """
    Returns base URL.
    params secret: API secret dictionary object.
    returns: base url for API call.
    """
    return f"{secret['protocol']}://{secret['host']}/{secret['resource']}"


def is_valid_response(response):
    """
    Checks for valid response from API.
    params response: Response from API.
    returns: True if response code is 200 - Success
    """
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.url}")
        return False
    return True

def is_contains_data(df):
    """
    Checks if the dataframe contains any data.
    params df: pandas dataframe as read from landed source data file.
    returns: boolean value.
    """
    if df.empty:
        return False
    return True


def is_columns_exists(df, column_list):
    """
    Checks if the columns exist in the dataframe.
    params df: pandas dataframe as read from landed source data file.
    returns: boolean value.
    """
    for column in column_list:
        if column not in df.columns:
            return False

    return True