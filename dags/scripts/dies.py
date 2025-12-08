import os
import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()


def fetch_json(url):
    """
    Helper function to fetch JSON data with error handling.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None


def process_data():
    """
    Fetches and transforms disease data from APIs.
    """
    print("Fetching global historical data...")
    hist_data = fetch_json("https://disease.sh/v3/covid-19/historical/all?lastdays=all")

    print("Fetching vaccine coverage data...")
    vac_coverage_data = fetch_json("https://disease.sh/v3/covid-19/vaccine/coverage?lastdays=all&fullData=true")

    print("Fetching country data...")
    country_data = fetch_json("https://disease.sh/v3/covid-19/countries")

    if not all([hist_data, vac_coverage_data, country_data]):
        print("Failed to fetch one or more datasets.")
        return None, None, None

    # Process Historical Data
    df_all = pd.DataFrame(hist_data)
    df_all.index = pd.to_datetime(df_all.index, format='mixed')
    df_all = df_all.reset_index().rename(columns={'index': 'date'})
    df_all = df_all[['date', 'cases', 'deaths', 'recovered']]

    # Process Vaccine Data
    df_vac = pd.DataFrame(vac_coverage_data)
    df_vac['date'] = pd.to_datetime(df_vac['date'], format='mixed')
    df_vac = df_vac[['date', 'daily', 'total']].rename(
        columns={'daily': 'daily_vaccinated', 'total': 'total_vaccinated'}
    )

    # Process Country Data
    df_country = pd.DataFrame(country_data)
    cols_to_keep = ['country', 'cases', 'deaths', 'recovered', 'active', 'population']
    df_country = df_country[cols_to_keep]

    return df_all, df_vac, df_country


def calculate_df_stats(dataframe):
    """
    Calculates basic statistics for numerical columns.
    """
    numerical_df = dataframe.select_dtypes(include=np.number)
    stats = {
        'min': numerical_df.min(),
        'max': numerical_df.max(),
        'mean': numerical_df.mean(),
        'sum': numerical_df.sum()
    }
    return pd.DataFrame(stats)


def save_to_db(df_all, df_vac, df_country):
    """
    Connects to MongoDB and saves the processed DataFrames.
    """
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        print("Error: MONGO_URI not set.")
        return

    try:
        client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
        db = client["testing_db"]

        # Collections
        disease_collection = db["disease_data"]
        vacc_coverage_collection = db["vac_data"]
        countries_collection = db["countries"]

        print("Saving Disease Data...")
        disease_collection.delete_many({})
        disease_collection.insert_many(df_all.to_dict(orient='records'))

        print("Saving Vaccine Data...")
        vacc_coverage_collection.delete_many({})
        vacc_coverage_collection.insert_many(df_vac.to_dict(orient='records'))

        print("Saving Country Data...")
        countries_collection.delete_many({})
        countries_collection.insert_many(df_country.to_dict(orient='records'))

        print("All data saved successfully.")
        client.close()

    except Exception as e:
        print(f"Database error: {e}")


def main():
    df_all, df_vac, df_country = process_data()
    if df_all is not None:
        save_to_db(df_all, df_vac, df_country)


if __name__ == "__main__":
    main()