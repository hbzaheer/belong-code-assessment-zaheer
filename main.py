import os
import requests
import json
import pandas as pd

import multiprocessing as mp
from multiprocessing import Pool, cpu_count

from helper_scripts import *
from secrets_manager.source_api_secret import api_secret


DEFAULT_LIMIT = 50000

base_url = f"{api_secret['protocol']}://{api_secret['host']}/{api_secret['resource']}"

def get_source_record_count(base_url, dataset):

    request_url = f'{base_url}/{dataset["resource_key"]}.json?$select=count(*)'
    print(request_url)

    response = requests.get(request_url)
    
    if is_valid_response(response):
        record_count = int(response.json()[0]["count"])
        print(record_count)
        return record_count
    else:
        raise Exception(f"Error: {response.status_code} - {response.url}")



def get_data_from_source(base_url, offset, limit, order):

    total_record_count = get_source_record_count(base_url, dataset)
    print(f'Total record count: {total_record_count}')

    return requests.get(f'{base_url}/{dataset["resource_key"]}.json?$limit={limit}&$offset={offset}&$order={order}').json()


def read_data_from_source(base_url, dataset):
        
    total_record_count = get_source_record_count(base_url, dataset)
    print(f'Total record count: {total_record_count}')

    limit = dataset["limit"] if "limit" in dataset and dataset["limit"] != "" else DEFAULT_LIMIT
    offset = 0
    all_data = []

    while total_record_count > offset:
        print(f'Reading from Offset {offset} ...')
        all_data += get_data_from_source(base_url, offset, limit, dataset["order"])
        offset += limit
        print(json.dumps(all_data, indent=4))
    
    return json.dumps(all_data)


def write_data_to_file(source, data):
    with open(f'{os.getcwd()}/landed_data/{source}.json', 'a') as f:
        f.write(data)



landing_configurations = json.load(open('landing_configurations.json', 'r'))
datasets = landing_configurations['datasets']


for dataset in datasets:
    
    print('Reading data from source ...')
    data = read_data_from_source(base_url, dataset)

    print('Writing data to file ...')
    write_data_to_file(dataset["source"], data)


# results_df = pd.DataFrame.from_records(all_data.json())

# print(f"Count: {results_df.count()[0]}")

# print(json.dumps(response.json(), indent=4))


