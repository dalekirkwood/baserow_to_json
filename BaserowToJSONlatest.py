#!/usr/bin/env python3

import requests
import json

def read_config():
    print("Reading configuration from config.txt...")
    config = {}
    with open('config.txt', 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            config[key.strip()] = value.strip()
    print("Configuration read successfully.")
    return config

def fetch_data(server_address, table_number, page, batch_size, token):
    print(f"Fetching data from server: Page {page}, Batch Size {batch_size}")
    url = f"{server_address}/api/database/rows/table/{table_number}/"
    headers = {"Authorization": f"Token {token}"}
    params = {
        "page": page,
        "size": batch_size,
        "user_field_names": "true"
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        print("Data fetched successfully.")
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
    return response.json()

def append_data_to_file(data, filename):
    print(f"Appending data to {filename}...")
    try:
        with open(filename, 'r+') as file:
            file_data = json.load(file)
            file_data.extend(data)
            file.seek(0)
            json.dump(file_data, file, indent=4)
    except FileNotFoundError:
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
    print("Data appended successfully.")

def update_status(current_page, filename='status.txt'):
    print(f"Updating status to page number {current_page} in {filename}...")
    with open(filename, 'w') as file:
        file.write(str(current_page))
    print("Status updated successfully.")

def main():
    print("Starting the data fetching process...")
    config = read_config()
    batch_size = int(config['batch size'])
    server_address = config['server address']
    table_number = config['table number']
    page_number = int(config['starting page number'])
    token = config['auth token']

    while True:
        print(f"Processing page number {page_number}")
        data = fetch_data(server_address, table_number, page_number, batch_size, token)
        if not data['results']:
            print("No more data to fetch.")
            break
        print(f"Page {page_number} fetched, {len(data['results'])} records found.")
        append_data_to_file(data['results'], 'database_dump.json')
        page_number += 1
        update_status(page_number)
        print(f"Preparing to process next page: {page_number}")

    print("All data has been successfully fetched and saved.")

if __name__ == "__main__":
    main()
