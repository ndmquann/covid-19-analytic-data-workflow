import os
import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne
import certifi
from dotenv import load_dotenv

load_dotenv()


def fetch_json(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def process_data():
    # INCREMENTAL CHANGE: Only fetch last 30 days to save bandwidth/time
    print("Fetching incremental data (last 30 days)...")

    # 1. Historical
    hist_data = fetch_json("https://disease.sh/v3/covid-19/historical/all?lastdays=30")

    # 2. Vaccine (API supports lastdays)
    vac_coverage_data = fetch_json("https://disease.sh/v3/covid-19/vaccine/coverage?lastdays=30&fullData=true")

    # 3. Country (This is always a snapshot of "Now", so we update current values)
    country_data = fetch_json("https://disease.sh/v3/covid-19/countries")

    if not all([hist_data, vac_coverage_data, country_data]):
        return None, None, None

    # Process Historical
    df_all = pd.DataFrame(hist_data)
    df_all.index = pd.to_datetime(df_all.index, format='mixed')
    df_all = df_all.reset_index().rename(columns={'index': 'date'})
    df_all = df_all[['date', 'cases', 'deaths', 'recovered']]

    # Process Vaccine
    df_vac = pd.DataFrame(vac_coverage_data)
    df_vac['date'] = pd.to_datetime(df_vac['date'], format='mixed')
    df_vac = df_vac[['date', 'daily', 'total']].rename(
        columns={'daily': 'daily_vaccinated', 'total': 'total_vaccinated'}
    )

    # Process Country
    df_country = pd.DataFrame(country_data)
    cols = ['country', 'cases', 'deaths', 'recovered', 'active', 'population']
    df_country = df_country[cols]

    return df_all, df_vac, df_country


def bulk_write_upsert(collection, data, unique_keys):
    """
    Generic helper to Upsert data.
    unique_keys: list of column names that make a row unique (e.g. ['date'] or ['country'])
    """
    if not data:
        return

    operations = []
    for record in data:
        # Build filter dynamically based on unique keys
        filter_query = {k: record[k] for k in unique_keys}

        operations.append(
            UpdateOne(filter_query, {"$set": record}, upsert=True)
        )

    if operations:
        collection.bulk_write(operations)
        print(f"Upserted {len(operations)} records into {collection.name}")


def save_to_db(df_all, df_vac, df_country):
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
    db = client["testing_db"]

    # 1. Historical Global: Unique Key is DATE
    bulk_write_upsert(db["disease_data"], df_all.to_dict(orient='records'), ['date'])

    # 2. Vaccine Global: Unique Key is DATE
    bulk_write_upsert(db["vac_data"], df_vac.to_dict(orient='records'), ['date'])

    # 3. Country Snapshot: Unique Key is COUNTRY
    # This updates the latest stats for the country
    bulk_write_upsert(db["countries"], df_country.to_dict(orient='records'), ['country'])

    client.close()


def main():
    df_all, df_vac, df_country = process_data()
    if df_all is not None:
        save_to_db(df_all, df_vac, df_country)


if __name__ == "__main__":
    main()