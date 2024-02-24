#!/usr/bin/env python3

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
import re

class Correlate:

    def __init__(self, prices_dataset, details_dataset, progress_bar):
        self.__details_dataset = details_dataset
        self.__prices_dataset = prices_dataset
        self.__progress_bar = progress_bar

    def __parse_list(self, s):
        try:
            values = re.findall('([^{"}\[\]]*)', s)
            out_values = []
            for v in values:
                n = [s for s in v.split(',') if s.strip() != '' and len(s) > 1]
                if n:
                    out_values.extend(n)
            return out_values
        except:
            return ["Unknown"]
        
    def __prepare_details(self, field_to_group, requires_explode=False):
        data_listings = pd.read_csv(self.__details_dataset)
        data_listings["airbnb_listing_id"] = data_listings["ad_id"]

        if field_to_group:
            if requires_explode:
                data_listings[field_to_group] = pd.Series(data_listings[field_to_group].apply(self.__parse_list))
                data_categories = pd.get_dummies(data_listings[field_to_group].explode()).groupby(level=0).sum()
                data_listings = pd.concat([data_listings["airbnb_listing_id"], data_categories], axis=1) 
            else:
                data_listings = pd.get_dummies(pd.concat([
                    data_listings["airbnb_listing_id"], data_listings[field_to_group]
                ], axis=1), columns=[field_to_group])

        return data_listings

    def __prepare_numeric_data(self):
        return dd.read_csv(self.__prices_dataset)

    def run(self, field_to_correlate, field_to_group=None, field_requires_explode=False):
        data_numeric = self.__prepare_numeric_data()
        data_listings = self.__prepare_details(field_to_group, field_requires_explode)
        data_joined = dd.merge(data_listings, data_numeric, on='airbnb_listing_id', how='inner')
        correlation = data_joined.corr(numeric_only=True)

        with self.__progress_bar():
            correlation = correlation[field_to_correlate].compute().abs().sort_values(ascending=False).drop([
                'price sum', 'price mean', 'airbnb_listing_id'
            ]).dropna()

        return correlation