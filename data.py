import pandas as pd
import numpy as np

import requests
import json
from dataclasses import dataclass

import sqlite3


@dataclass
class dataset_details:
    datasetId: str
    comments: str

@dataclass
class gov_sg_data_ref:
    hdb = dataset_details(
        datasetId = 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc',
        comments = '2017-01 onwards'
    )

    hdb0 = dataset_details(
        datasetId = 'd_ebc5ab87086db484f88045b47411ebc5',
        comments = '1990-01 to 1999-12'
    )

    hdb1 = dataset_details(
        datasetId = 'd_43f493c6c50d54243cc1eab0df142d6a',
        comments = '2000-01 to 2012-02'
    )

    hdb2 = dataset_details(
        datasetId = 'd_2d5ff9ea31397b66239f245f57751537',
        comments = '2012-03 to 2014-12'
    )

    hdb3 = dataset_details(
        datasetId = 'd_ea9ed51da2787afaf8e51f827c304208',
        comments = '2015-01 to 2016-12'
    )


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
        con.execute(table_creation)


def gov_data_puller(
        datasetId: str = 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'
    ) -> pd.DataFrame:
    url = f'https://api-open.data.gov.sg/v1/public/api/datasets/{datasetId}/initiate-download'
    response = requests.get(url=url)
    result = response.json()

    if response.status_code != 201:
        raise requests.exceptions.HTTPError(result['errorMsg'])

    return pd.read_csv(result['data']['url'])


def parse_remaining_lease(
        remaining_lease: pd.Series
    ) -> pd.Series:
    leases = remaining_lease.str.split('years', expand=True)
    leases[1] = leases[1].str.split(expand=True).fillna(0)[0]
    leases = leases.astype(int)
    return leases[0] * 12 + leases[1]


if __name__ == '__main__':
    main()