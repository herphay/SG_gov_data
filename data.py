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


def parse_transaction_month(
        month: pd.Series
    ) -> pd.Series:
    dates = month.str.split('-', expand=True)
    dates = dates.astype(int)
    return dates[0] * 12 + dates[1]


def find_lease_start_date(
        method: str = 'weighted_avg'
    ) -> pd.DataFrame:
    latest_resale = gov_data_puller(datasetId='d_8b84c4ee58e3cfc0ece0d773c8ca6abc')
    latest_resale['rlease_mth'] = parse_remaining_lease(latest_resale['remaining_lease'])
    latest_resale['tdate_mth'] = parse_transaction_month(latest_resale['month'])
    latest_resale['lease_start_mth'] =  latest_resale['tdate_mth'] - \
                                        (99 * 12 - latest_resale['rlease_mth'])
    
    # create lease start group
    grpdx = ['street_name', 'block']
    lease_start_grp = latest_resale.groupby(grpdx)['lease_start_mth']
    
    match method:
        # Get self defined median
        case 'unique_median':
            std_lease_start =  lease_start_grp.unique().apply(find_median_lease_start).reset_index()

        # get weighted avg start
        case 'weighted_avg':
            sl_freq = lease_start_grp.value_counts().reset_index()
            sl_freq['prod'] = sl_freq['lease_start_mth'] * sl_freq['count']
            sl_freq = sl_freq.groupby(grpdx)
            sl_freq = (sl_freq['prod'].sum() / sl_freq['count'].sum()).round().astype(int)
            std_lease_start = sl_freq.reset_index()
        
        # get most frequent
        case 'mode':
            ...


    # To merge
    # a = pd.merge(latest_resale, std_lease_start, how='left', on=grpdx)
    return std_lease_start


def find_median_lease_start(
        start_dates: np.ndarray
    ) -> int:
    """
    start_dates should be a sorted list of int representation of the lease start date
    """
    # If there is only 1 lease start date
    if start_dates.size == 1:
        return start_dates[0]
    
    # The sort will affect underlying np arrays -> the arrays in the df are passed by reference
    start_dates.sort()
    deltas = np.diff(start_dates)

    # If all lease starts are in the same conseq run
    if (deltas == 1).all():
        return np.median(start_dates)
    
    current_run_pos = 0
    current_run_len = 1
    max_run_pos = 0
    max_run_len = 1

    for i in range(deltas.size):
        if deltas[i] == 1:
            current_run_len += 1
        else:
            if current_run_len > 
    # start_dates = start_dates.tolist()
    # if (nums := len(start_dates)) == 1:
    #     return start_dates[0]
    # else:
    #     i = 0
    #     in_conseq = False
    #     for _ in range(nums - 1):
    #         if start_dates[i + 1] - start_dates[i] == 1:
    #             in_conseq = True
    #             i += 1
    #         elif in_conseq:
    #             del start_dates[i + 1:]
    #             break
    #         else:
    #             del start_dates[i]

    #     return int(np.median(start_dates))


if __name__ == '__main__':
    main()