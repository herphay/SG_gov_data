import pandas as pd
import numpy as np

import requests
import json

from typing import Literal
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


def pull_all_hdb_data(
        ls_method: Literal[
            'weighted_avg',
            'longest_median',
            'mode'
        ] = 'weighted_avg'
    ) -> pd.DataFrame:
    datasetIds = [
        gov_sg_data_ref.hdb0.datasetId,
        gov_sg_data_ref.hdb1.datasetId,
        gov_sg_data_ref.hdb2.datasetId,
        gov_sg_data_ref.hdb3.datasetId,
        gov_sg_data_ref.hdb.datasetId,
                  ]
    data_list = [gov_data_puller(datasetId) for datasetId in datasetIds]
    resales = pd.concat(data_list[:4], join='inner', ignore_index=True)
    
    # Update latest resale df to standard convention
    data_list[4]['remaining_lease'] = parse_remaining_lease(data_list[4]['remaining_lease'])
    resales = pd.concat([resales, data_list[4]], ignore_index=True)

    # standardize all date conventions
    resales['month'] = parse_transaction_month(resales['month'])
    resales['lease_commence_date'] = resales['lease_commence_date'] * 12
    
    # Find and merge lease_start_date
    lsd = find_lease_start_date(method=ls_method)
    resales = pd.merge(resales, lsd, 'left', on=['street_name', 'block'])
    
    return resales


def database_setup() -> None:
    resales = pull_all_hdb_data()

    with sqlite3.connect('hdb_data.db') as con:
        table_creation = """
        CREATE TABLE IF NOT EXISTS resales (
            transaction_id          INTEGER PRIMARY KEY,
            month                   INTEGER,
            town                    TEXT,
            flat_type               TEXT,
            block                   TEXT,
            street_name             TEXT,
            storey_range            TEXT,
            floor_area_sqm          FLOAT,
            flat_model              TEXT,
            lease_commence_date     INTEGER,
            remaining_lease         INTEGER,
            resale_price            FLOAT,
            lease_start_mth         INTEGER
        );
        """
        con.execute(table_creation)
        # resales.to_sql('resales', con=con, if_exists='replace')
    
    db_insertion(resales)


def db_insertion(
        resale_data: pd.DataFrame,
    ) -> None:
    cols = ['month', 'town', 'flat_type', 'block', 'street_name', 'storey_range', 
            'floor_area_sqm', 'flat_model', 'lease_commence_date', 'resale_price', 
            'remaining_lease', 'lease_start_mth']
    if resale_data.shape[1] != 12: # or ~(resale_data.columns == cols).all()
        raise ValueError('Incorrect number of columns in DataFrame')
    
    try:
        resale_data = resale_data[cols]
    except KeyError:
        raise ValueError('Column sequence error')

    # f2icols = ['remaining_lease', 'lease_start_mth']
    # resale_data[f2icols] = resale_data[f2icols].astype(pd.Int64Dtype())
    rows = resale_data.shape[0]
    resale_data = list(resale_data.itertuples(index=False, name=None))

    insertion_sql = """
    INSERT INTO resales (
        month,
        town,
        flat_type,
        block,
        street_name,
        storey_range,
        floor_area_sqm,
        flat_model,
        lease_commence_date,
        resale_price,
        remaining_lease,
        lease_start_mth
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    with sqlite3.connect('hdb_data.db') as con:
        con.executemany(insertion_sql, resale_data)
    print(f'Successfully inserted {rows} rows into the resales database')


def update_resale_data() -> None:
    """
    Pull the latest HDB resale data and update new entries into the database
    """
    with sqlite3.connect('hdb_data.db') as con:
        sql = """
        SELECT *
        FROM resales
        WHERE month = (SELECT MAX(month) FROM resales)
        """
        latest_trans = pd.read_sql_query(sql, con)
    
    latest_resale = gov_data_puller()
    lsd = find_lease_start_date(method='weighted_avg', latest_resale=latest_resale)
    
    latest_resale['month'] = parse_transaction_month(latest_resale['month'])
    latest_resale = latest_resale.loc[latest_resale['month'] >= latest_trans['month'][0]]

    latest_resale['lease_commence_date'] = latest_resale['lease_commence_date'] * 12
    latest_resale['remaining_lease'] = parse_remaining_lease(latest_resale['remaining_lease'])
    
    latest_resale = pd.merge(latest_resale, lsd, 'left', on=['street_name', 'block'])

    f2icols = ['remaining_lease', 'lease_start_mth']
    # latest_trans[f2icols] = latest_trans[f2icols].astype(int)
    latest_trans['fingerprint'] = latest_trans[latest_resale.columns].astype(str).agg(''.join, axis=1)
    latest_resale['fingerprint'] = latest_resale.astype(str).agg(''.join, axis=1)

    latest_resale = pd.merge(latest_resale, latest_trans[['fingerprint', 'transaction_id']], 
                             'left', on='fingerprint')
    latest_resale = latest_resale.loc[latest_resale['transaction_id'].isna()]
    # return latest_resale[latest_trans.columns[1:-1]]
    db_insertion(latest_resale[latest_trans.columns[1:-1]])


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
    #return df with each splitted item in col named 0, 1...
    dates = month.str.split('-', expand=True)
    dates = dates.astype(int)
    return dates[0] * 12 + dates[1]


def find_lease_start_date(
        method: Literal[
            'weighted_avg',
            'longest_median',
            'mode'
            ] = 'weighted_avg',
        latest_resale: pd.DataFrame | None = None
    ) -> pd.DataFrame:
    if latest_resale is None:
        latest_resale = gov_data_puller(datasetId='d_8b84c4ee58e3cfc0ece0d773c8ca6abc')
    else:
        latest_resale = latest_resale.copy()
    latest_resale['rlease_mth'] = parse_remaining_lease(latest_resale['remaining_lease'])
    latest_resale['tdate_mth'] = parse_transaction_month(latest_resale['month'])
    latest_resale['lease_start_mth'] =  latest_resale['tdate_mth'] - \
                                        (99 * 12 - latest_resale['rlease_mth'])
    
    # create lease start group
    grpdx = ['street_name', 'block']
    lease_start_grp = latest_resale.groupby(grpdx)['lease_start_mth']
    
    match method:
        # Get self defined median
        case 'longest_median':
            std_lease_start =  lease_start_grp.unique().apply(find_median_lease_start).reset_index()

        # get weighted avg start
        case 'weighted_avg':
            sl_freq = lease_start_grp.value_counts().reset_index()
            sl_freq['prod'] = sl_freq['lease_start_mth'] * sl_freq['count']
            sl_freq = sl_freq.groupby(grpdx)
            sl_freq = (sl_freq['prod'].sum() / sl_freq['count'].sum()).round().astype(int)
            std_lease_start = sl_freq.reset_index()
            std_lease_start.columns = grpdx + ['lease_start_mth']
        
        # get most frequent
        case 'mode':
            sl_freq = lease_start_grp.value_counts().reset_index()
            max_count = sl_freq.groupby(grpdx)['count'].transform('max')
            sl_freq = sl_freq.loc[sl_freq['count'] == max_count]
            sl_freq = sl_freq.sort_values(grpdx + ['lease_start_mth'])
            std_lease_start = sl_freq.drop_duplicates(grpdx)[grpdx + ['lease_start_mth']]

    # To merge
    # a = pd.merge(latest_resale, std_lease_start, how='left', on=grpdx)
    return std_lease_start


def find_median_lease_start(
        start_dates: np.ndarray
    ) -> int:
    """
    start_dates is the int representation (yyyy-mm converted to mth) of the lease start date

    Finds the median lease start date in the longest conseq. run of lease starts
     - If multiple runs have the same length, take the earlier run 
     - (biased to give lower remaining lease)
    """
    # If there is only 1 lease start date
    if start_dates.size == 1:
        return start_dates[0]
    
    # The sort will affect underlying np arrays -> the arrays in the df are passed by reference
    start_dates.sort()
    deltas = np.diff(start_dates)

    # If all lease starts are in the same conseq run
    if (deltas == 1).all():
        return round(np.median(start_dates))
    
    current_run_pos = 0
    current_run_len = 1
    max_run_pos = 0
    max_run_len = 1

    last_pos = deltas.size - 1
    for i in range(last_pos + 1):
        # If run is still ongoing
        if deltas[i] == 1:
            current_run_len += 1
            if i < last_pos:
                continue
        # If run terminates or we are at the last pos -> check max run
        # Update run to be max run if its the longest
        if current_run_len > max_run_len:
            max_run_len = current_run_len
            max_run_pos = current_run_pos

        # Update a new current run (last pos and delta == 1 will get updated but irrelevant)
        current_run_pos = i + 1
        current_run_len = 1
        
    return round(np.median(start_dates[max_run_pos:max_run_pos + max_run_len]))
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


def lease_start_method_tester():
    methods = ['weighted_avg', 'longest_median', 'mode']
    results = [pull_all_hdb_data(method) for method in methods]
    print('all data pulled')

    for m, df in zip(methods, results):
        df = df.loc[~df['remaining_lease'].isna(), ['month', 'lease_start_mth', 'remaining_lease']]
        # +ve delta means calculated remaining lease is longer than actual
        # -ve delta means calculated remaining lease is shorter than actual
        df['delta'] = df['lease_start_mth'] + 99 * 12 - df['month'] - df['remaining_lease']
        
        summary = df['delta'].value_counts().sort_index()

        print(f"\nFor method '{m}':")
        print(f'Variance is: {df['delta'].var():.3f}')
        print(f'0mth % is: {summary.loc[abs(summary.index) < 1].sum() / summary.sum() * 100:.2f}%')
        print(f'1mth % is: {summary.loc[abs(summary.index) < 2].sum() / summary.sum() * 100:.2f}%')
        print(f'2mth % is: {summary.loc[abs(summary.index) < 3].sum() / summary.sum() * 100:.2f}%')

    return results
    # For method weighted_avg:
    # Variance is: 0.491
    # 0mth % is: 56.03%
    # 1mth % is: 99.13%
    # 2mth % is: 99.85%
    # For method longest_median:
    # Variance is: 0.545
    # 0mth % is: 50.53%
    # 1mth % is: 98.45%
    # 2mth % is: 99.82%
    # For method mode:
    # Variance is: 0.563
    # 0mth % is: 57.71%
    # 1mth % is: 97.70%
    # 2mth % is: 99.75%


if __name__ == '__main__':
    main()