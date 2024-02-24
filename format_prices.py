#!/usr/bin/env python3
import dask.dataframe as dd
import numpy as np
from scipy import stats
from dask.diagnostics import ProgressBar

ProgressBar().register()

def generate_derived_files():
    data_stays = dd.read_csv('dataset/Price_AV_Itapema.csv', dtype={
        "airbnb_listing_id": "int64",
        "date": "str",
        "price": "float64",
        "price_string": "str",
        "minimum_stay": "int64",
        "available": "str",
        "aquisition_date": "str",
        "av_for_checkin": "str",
        "av_for_checkout": "str",
        "index": "str",
        "bookable": "str",
        "ano": "int64",
        "mes": "int64",
        "dia": "int64"
    })
    data_stays['date'] = dd.to_datetime(data_stays['date'])
    data_stays['ano_listing'] = data_stays['date'].dt.year
    data_stays = data_stays.drop_duplicates(subset=['airbnb_listing_id', 'date'])

    with ProgressBar():
        filtered_stays = data_stays[
            (data_stays["available"].str.lower() == "false")
        ]
        grouped_stays = filtered_stays.groupby(['airbnb_listing_id', 'ano_listing']).agg({
            "price": ['mean', 'sum', 'count']
        }).compute()
        grouped_stays = grouped_stays.reset_index()
        
        # Retira outliers (parecem erros de cadastro)
        grouped_stays = grouped_stays[(np.abs(stats.zscore(grouped_stays)) < 3).all(axis=1)]
        grouped_stays.columns = [' '.join(col).strip() for col in grouped_stays.columns.values]
        grouped_by_listings = grouped_stays.groupby('airbnb_listing_id').agg({
            "price sum": "sum", 
            "price mean": "mean"
        })
    
    filename_per_year = 'dataset/Price_grouped_Year.csv'
    filename_prices = 'dataset/Price_grouped.csv' 

    grouped_stays.to_csv(filename_per_year)
    grouped_by_listings.to_csv(filename_prices)

    return [filename_prices, filename_per_year]