import os
import requests
import json
from time import time
import pandas as pd

from multiprocessing import Pool, cpu_count

from helper_scripts import is_valid_response
from secrets_manager.source_api_secret import api_secret


DEFAULT_LIMIT = 50000

base_url = f"{api_secret['protocol']}://{api_secret['host']}/{api_secret['resource']}"

def get_source_record_count(base_url, dataset):

    request_url = f'{base_url}/{dataset["resource_key"]}.json?$select=count(*)'
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


def read_data_from_source(base_url, dataset, total_record_count):

    limit = dataset["limit"] if "limit" in dataset and dataset["limit"] != "" else DEFAULT_LIMIT
    request_url = f'{base_url}/{dataset["resource_key"]}.json'

    p = Pool(processes=int(cpu_count()/2))

    args = []
    for i in range(0, total_record_count + 1, limit):
        args.append((request_url, i, limit, dataset["order"]))

    result = p.starmap(get_data_from_source, args)

    p.close()
    p.join()

    return json.dumps(result)


def write_data_to_file(source, data):
    with open(f'{os.getcwd()}/landed_data/{source}.json', 'w') as f:
        f.write(data)

if __name__ == '__main__':

    landing_configurations = json.load(open('landing_configurations.json', 'r'))
    datasets = landing_configurations['datasets']


    for dataset in datasets:
        
        total_record_count = get_source_record_count(base_url, dataset)
        print(f'Total record count: {total_record_count}')
        
        print('Reading data from source ...')
        start_time = time()
        data = read_data_from_source(base_url, dataset, total_record_count)
        end_time = time()
        print(f'Time taken to read data from source {dataset["source"]}: {end_time - start_time}')

        print('Writing data to file ...')
        start_time = time()
        write_data_to_file(dataset["source"], data)
        end_time = time()
        print(f'Time taken to write data to file {dataset["source"]}: {end_time - start_time}')


