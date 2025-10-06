import pandas as pd
import numpy as np

import requests
import json

import sqlite3

def main():
    ...


def database_setup():
    with sqlite3.connect('hdb_data.db') as con:
        table_creation = """
        CREATE TABLE IF NOT EXISTS resale (
            transaction_id INTEGER PRIMARY KEY,
            month TEXT,
            town TEXT,
            flat_type TEXT,
            block TEXT,
            street_name TEXT,
            storey_range TEXT,
            floor_area_sqm FLOAT,
            flat_model TEXT,
            lease_commence_date INTEGER,
            remaining_lease INTEGER,
            resale_price FLOAT
        );
        """


def gov_data_puller(
        datasetId: str = 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'
    ) -> pd.DataFrame:
    url = f'https://api-open.data.gov.sg/v1/public/api/datasets/{datasetId}/initiate-download'
    response = requests.get(url=url)
    result = response.json()

    if response.status_code != 201:
        raise requests.exceptions.HTTPError(result['errorMsg'])

    return pd.read_csv(result['data']['url'])



if __name__ == '__main__':
    main()