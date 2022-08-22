import os
import requests
import json
from time import time
import pandas as pd

from multiprocessing import Pool, cpu_count
from stats import get_top_n_records_by_grain

from helper_scripts import is_valid_response, prepare_base_url
from secrets_manager.secrets import api_secret
from tests.test_stats import *


BASE_URL = prepare_base_url(api_secret)
DEFAULT_LIMIT = 50000

def get_source_record_count(dataset):

    request_url = f'{BASE_URL}/{dataset["resource_key"]}.json?$select=count(*)'
    print(request_url)

    response = requests.get(request_url)
    
    if is_valid_response(response):
        record_count = int(response.json()[0]["count"])
        return record_count
    else:
        raise Exception(f"Error: {response.status_code} - {response.url}")


def get_data_from_source(request_url, offset, limit, order):

    print(f'Reading from Offset {offset} ...')
    return requests.get(f'{request_url}?$limit={limit}&$offset={offset}&$order={order}').json()


def read_data_from_source(dataset, total_record_count):
    """
    Reads data from a source.
    param base_url: The base url of the source API.
    param dataset: The dataset to be read from the source.
    param total_record_count: The total number of records in the source dataset.
    return: The data read from the source.
    """
    limit = dataset["limit"] if "limit" in dataset and dataset["limit"] != "" else DEFAULT_LIMIT
    request_url = f'{BASE_URL}/{dataset["resource_key"]}.json'

    p = Pool(processes=int(cpu_count()/2))

    args = []
    for i in range(0, total_record_count + 1, limit):
        args.append((request_url, i, limit, dataset["order"]))

    result = p.starmap(get_data_from_source, args)
    data = []
    for r in result:
        data.extend(r)

    p.close()
    p.join()

    return json.dumps(data, indent=4)


def write_data_to_file(source_name, data):
    """
    Writes data to a file.
    param source: The source dataset name.
    param data: The data to be written.
    """
    with open(f'{os.getcwd()}/landed_data/{source_name}.json', 'w') as f:
        f.write(data)


if __name__ == '__main__':

    landing_configurations = json.load(open('landing_configurations.json', 'r'))
    datasets = landing_configurations['datasets']

    for dataset in datasets:
        
        total_record_count = get_source_record_count(dataset)
        print(f'Total record count: {total_record_count}')
        
        print('Reading data from source ...')
        start_time = time()
        data = read_data_from_source(dataset, total_record_count)
        end_time = time()
        print(f'Time taken to read data from source {dataset["source_name"]}: {end_time - start_time}')

        print('Writing data to file ...')
        start_time = time()
        write_data_to_file(dataset["source_name"], data)
        end_time = time()
        print(f'Time taken to write data to file {dataset["source_name"]}: {end_time - start_time}')


    print('Top 10 by day ...')
    df = pd.read_json(f'{os.getcwd()}/landed_data/monthly_counts_per_hour.json')
    print(get_top_n_records_by_grain(df, 'day', 'sensor_name', 'hourly_counts', 10))

    print('Top 10 by day ...')
    df = pd.read_json(f'{os.getcwd()}/landed_data/monthly_counts_per_hour.json')
    print(get_top_n_records_by_grain(df, 'day', 'sensor_name', 'hourly_counts', 10))

    

