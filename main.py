#!/usr/bin/env python3
from format_prices import generate_derived_files
from corr import Correlate
from cluster import Cluster
from plotter import Plotter
from dask.diagnostics import ProgressBar
import warnings
from pathlib import Path

# Supress warnings
warnings.filterwarnings("ignore")
ProgressBar().register()
Path("./output").mkdir(parents=True, exist_ok=True)

# print("Preparing files...")
[prices_dataset, prices_per_year_dataset] = generate_derived_files()

cluster = Cluster(
    geolocation_dataset="dataset/Mesh_Ids_Data_Itapema.csv",
    prices_per_year_dataset=prices_per_year_dataset,
    progress_bar=ProgressBar,
    num_clusters=20
)

corr = Correlate(
    prices_dataset=prices_dataset, 
    details_dataset="dataset/Details_Data.csv",
    progress_bar=ProgressBar
)
plotter = Plotter()

# print("Generate clusters...")
cluster.generate_clusters("output/clusters.html")
statistics = cluster.generate_statistics("output/statistics.txt")

plotter.plot_bars(
    statistics["cluster-year"].head(20), 
    statistics["estimated revenue"].head(20), 
    "Cluster Year", 
    "Estimated Revenue", 
    "Yearly Estimated Revenue by Cluster",
    output_file="output/estimated_revenues_by_cluster_year.png"
)

plotter.plot_bars(
    statistics["cluster-year"].head(20), 
    statistics["# listings"].head(20), 
    "Cluster Year", 
    "# Listings", 
    "Yearly Number of Listings by Cluster",
    output_file="output/number_listings_by_cluster_year.png"
)

print("Plotting Safety Features...")
plotter.plot(
    corr.run(field_to_correlate='price sum', field_to_group='safety_features', field_requires_explode=True),
    'Revenues',
    'Safety Features',
    output_file="output/safety_features.png"
)

print("Plotting House Rules...")
plotter.plot(
    corr.run(field_to_correlate='price sum', field_to_group='house_rules', field_requires_explode=True),
    'Revenues',
    'House Rules',
    output_file="output/house_rules.png"
)

print("Plotting Amenities...")
plotter.plot(
    corr.run(field_to_correlate='price sum', field_to_group='amenities', field_requires_explode=True),
    'Revenues',
    'Amenities',
    output_file="output/amenities.png"
)

print("Plotting Listing Types...")
plotter.plot(
    corr.run(field_to_correlate='price sum', field_to_group='listing_type', field_requires_explode=False),
    'Revenues',
    'Listing Types',
    output_file="output/listing_types.png"
)

print("Finished.")