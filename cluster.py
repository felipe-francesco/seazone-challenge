#!/usr/bin/env python3

import pandas as pd
import dask.dataframe as dd
from sklearn.cluster import KMeans
import folium
from folium.features import DivIcon

class Cluster:

    def __init__(self, geolocation_dataset, prices_per_year_dataset, progress_bar, num_clusters=20):
        self.__geolocation_dataset = geolocation_dataset
        self.__prices_per_year_dataset = prices_per_year_dataset
        self.__num_clusters = num_clusters
        self.__progress_bar = progress_bar

    def __save_map(self, centroides, outuput_clusters):
        mapa = folium.Map(location=[self.__data_geo['latitude'].mean(), self.__data_geo['longitude'].mean()], zoom_start=12)

        for idx, centroide in enumerate(centroides):
            folium.Marker([centroide[0], centroide[1]], icon=DivIcon(
                icon_size=(250,36),
                icon_anchor=(0,0),
                html=f'<div style="font-size: 10pt">Cluster {idx}</div>',
            )).add_to(mapa)

        mapa.save(outuput_clusters)
    
    def generate_clusters(self, outuput_clusters):
        self.__data_geo = pd.read_csv(self.__geolocation_dataset)

        X = self.__data_geo[['latitude', 'longitude']]

        kmeans = KMeans(n_clusters=self.__num_clusters, random_state=42)
        kmeans.fit(X)

        self.__data_geo['cluster'] = kmeans.labels_
        self.__save_map(kmeans.cluster_centers_, outuput_clusters)
    
    def generate_statistics(self, output_file):
        data_valores = dd.read_csv(self.__prices_per_year_dataset)
        data_completo = dd.merge(self.__data_geo, data_valores, on='airbnb_listing_id', how='inner')

        with self.__progress_bar():
            estatisticas_por_cluster = data_completo.groupby(['cluster', 'ano_listing']).agg({
                'airbnb_listing_id': 'count',
                'price mean': ['mean', dd.Aggregation('median', chunk=lambda s: s.quantile(0.5), agg=lambda s: s.median())],
                'price sum': ['mean', dd.Aggregation('median', chunk=lambda s: s.quantile(0.5), agg=lambda s: s.median())]
            }).compute()
            estatisticas_por_cluster = estatisticas_por_cluster.reset_index()
            estatisticas_por_cluster.columns = [' '.join(col).strip() for col in estatisticas_por_cluster.columns.values]
            estatisticas_por_cluster["# listings"] = estatisticas_por_cluster["airbnb_listing_id count"]
            estatisticas_por_cluster = estatisticas_por_cluster.drop(columns=['airbnb_listing_id count'])
            estatisticas_por_cluster = estatisticas_por_cluster.sort_values(by=["price sum mean"], ascending=False)
            estatisticas_por_cluster.to_csv(output_file)
            estatisticas_por_cluster["cluster-year"] = estatisticas_por_cluster['cluster'].astype(str) +"-"+ estatisticas_por_cluster['ano_listing'].astype(str)
            estatisticas_por_cluster["estimated revenue"] = estatisticas_por_cluster['price sum mean']
            estatisticas_por_cluster = estatisticas_por_cluster.drop(
                columns=["price mean mean", "price mean median", "price sum median", "cluster", "ano_listing", "price sum mean"]
            )
            return estatisticas_por_cluster
