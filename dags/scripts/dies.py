import pandas as pd
import numpy as np
import json
import requests
from pymongo import MongoClient
from datetime import datetime
import matplotlib.pyplot as plt
import copy
import os
from dotenv import load_dotenv # pip install python-dotenv

load_dotenv() # Load variables from .env file

# Time-series global
hist_url = "https://disease.sh/v3/covid-19/historical/all?lastdays=all"
hist_data = requests.get(hist_url).json() # include case, death, recovered

# Vaccine global
vac_coverage_url = "https://disease.sh/v3/covid-19/vaccine/coverage?lastdays=all&fullData=true"
vac_coverage_data = requests.get(vac_coverage_url).json()

# country total cdr
country_url = "https://disease.sh/v3/covid-19/countries"
country_data = requests.get(country_url).json()


df_all = pd.DataFrame(hist_data)
df_all.index = pd.to_datetime(df_all.index, format='mixed')
df_all = df_all.reset_index()
df_all = df_all.rename(columns={'index':'date'})
df_all = df_all[['date', 'cases', 'deaths', 'recovered']]

df_vac = pd.DataFrame(vac_coverage_data)
df_vac['date'] = pd.to_datetime(df_vac['date'], format='mixed')
df_vac = df_vac[['date', 'daily', 'total']]
df_vac = df_vac.rename(columns={'daily': 'daily_vaccinated', 'total': 'total_vaccinated'})

df_country = pd.DataFrame(country_data)
df_country = df_country[['country', 'cases', 'deaths', 'recovered', 'active', 'population']]

def calculate_df_stats(dataframe):
    # Select only numerical columns
    numerical_df = dataframe.select_dtypes(include=np.number)

    # Calculate statistics
    stats = {
      'min': numerical_df.min(),
      'max': numerical_df.max(),
      'mean': numerical_df.mean(),
      'sum': numerical_df.sum()
    }

    # Create a new DataFrame from the statistics
    stats_df = pd.DataFrame(stats)

    return stats_df

# Connect
mongo_uri = os.getenv("MONGO_URI") # Get the URI securely
client = MongoClient(mongo_uri)
# Init
db = client["testing_db"]
disease_collection = db["disease_data"]
vacc_coverage_collection = db["vac_data"]
countries_collection = db["countries"]

# Load
# case, death, recover
cdr = df_all.to_dict(orient='records')
disease_collection.delete_many({})
disease_collection.insert_many(cdr)
# vaccine
vac = df_vac.to_dict(orient='records')
vacc_coverage_collection.delete_many({})
vacc_coverage_collection.insert_many(vac)
# country info
country = df_country.to_dict(orient='records')
countries_collection.delete_many({})
countries_collection.insert_many(country)

