import pandas as pd
import numpy as np
import os
import certifi
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_data(db_name, collection_name, uri, flatten=False):
    """
    Fetches data. If flatten=True, unpacks nested dictionaries into separate columns.
    Example: {'cases': {'total': 10}} becomes column 'cases_total': 10
    """
    client = MongoClient(uri, tlsCAFile=certifi.where(), connectTimeoutMS=30000)
    db = client[db_name]
    data = list(db[collection_name].find({}, {"_id": 0}))
    client.close()

    if not data:
        return pd.DataFrame()

    if flatten:
        # json_normalize flattens nested dicts. sep='_' makes 'cases.total' -> 'cases_total'
        print(f"   -> Flattening nested data for {collection_name}...")
        return pd.json_normalize(data, sep='_')

    return pd.DataFrame(data)

def check(native_coll, opt_coll, unique_keys, numeric_cols, flatten=False):
    mongo_uri = os.getenv("MONGO_URI")

    print(f"--- COMPARING: {native_coll} (Native) vs {opt_coll} (Optimized) ---")

    try:
        df_native = get_data("testing_db", native_coll, mongo_uri, flatten)
        df_opt = get_data("testing_db", opt_coll, mongo_uri, flatten)
    except Exception as e:
        print(f"Connection Failed: {e}")
        return

    if df_native.empty or df_opt.empty:
        print("One of the collections is empty. Cannot compare.")
        return

        # Debug: Print columns if flattening happened so user knows new names
    if flatten:
        print(f"   -> Columns available after flattening: {list(df_native.columns)} ...")

    if 'date' in df_native.columns:
        df_native['date'] = pd.to_datetime(df_native['date'])
        df_opt['date'] = pd.to_datetime(df_opt['date'])

    if df_native.duplicated(subset=unique_keys).any():
        df_native = df_native.drop_duplicates(subset=unique_keys, keep='last')
    if df_opt.duplicated(subset=unique_keys).any():
        df_opt = df_opt.drop_duplicates(subset=unique_keys, keep='last')

    try:
        df_native = df_native.set_index(unique_keys).sort_index()
        df_opt = df_opt.set_index(unique_keys).sort_index()
    except KeyError as e:
        print(f"Skipping: Key {e} not found in columns.")
        return

    # 3. Find Missing Rows (Set Difference)
    native_idx = set(df_native.index)
    opt_idx = set(df_opt.index)

    missing_in_opt = native_idx - opt_idx
    extra_in_opt = opt_idx - native_idx

    print(f"Rows in Native: {len(df_native)}")
    print(f"Rows in Optimized: {len(df_opt)}")
    print(f"Missing in Optimized: {len(missing_in_opt)}")
    print(f"Extra in Optimized: {len(extra_in_opt)}")

    # Align datasets to only shared indices for value comparison
    shared_idx = native_idx.intersection(opt_idx)

    # Select only common columns to compare
    common_cols = list(set(df_native.columns).intersection(df_opt.columns))

    df1 = df_native.loc[list(shared_idx), common_cols].sort_index()
    df2 = df_opt.loc[list(shared_idx), common_cols].sort_index()

    for col in numeric_cols:
        if col in df1.columns:
            # errors='coerce' turns "abc" or "" into NaN, and "123" into 123.0
            df1[col] = pd.to_numeric(df1[col], errors='coerce')
            df1[col] = df1[col].fillna(0).round(2)
        else:
            print(f"Missing {col}")

        if col in df2.columns:
            df2[col] = pd.to_numeric(df2[col], errors='coerce')
            df2[col] = df2[col].fillna(0).round(2)
        else:
            print(f"Missing {col}")


    # Run Comparison
    # This checks if values are EXACTLY equal
    diff_mask = (df1 != df2).stack()

    # Filter only True differences (where values don't match)
    # Note: NaNs don't equal NaNs in Python, so we handle that:
    diffs = df1.compare(df2)

    if diffs.empty:
        print("SUCCESS: Shared rows match perfectly!")
    else:
        print(f"FOUND DIFFERENCES in {len(diffs)} rows.")
        print("Sample Differences (Native vs Optimized):")
        print(diffs.head())

def main():
    # Example 1: Compare Vaccinations
    # Keys: location, date. Numeric: daily_vaccinations
    check(
        native_coll="countries",  # Your Native Collection Name
        opt_coll="countries",    # Your Optimized Collection Name
        unique_keys=["country"],
        numeric_cols=["cases", "deaths", "recovered", "active", "population"]
    )

    print("\n" + "="*30 + "\n")

    check(
        native_coll="case_agesex_collection",  # Your Native Collection Name
        opt_coll="case_agesex_collection",  # Your Optimized Collection Name
        unique_keys=["country", "date", "age_begin", "age_end"],
        numeric_cols=["casesF", "casesM", "populationin1000sF", "populationin1000sM"]
    )

    print("\n" + "=" * 30 + "\n")

    check(
        native_coll="death_agesex_collection",  # Your Native Collection Name
        opt_coll="death_agesex_collection",  # Your Optimized Collection Name
        unique_keys=["country", "date", "age_begin", "age_end"],
        numeric_cols=["deathsF", "deathsM", "populationin1000sF", "populationin1000sM"]
    )

    print("\n" + "=" * 30 + "\n")

    check(
        native_coll="vac_agesex_collection",  # Your Native Collection Name
        opt_coll="vac_agesex_collection",  # Your Optimized Collection Name
        unique_keys=["country", "date", "age_begin", "age_end"],
        numeric_cols=[
            "vac1_vacsF",
            "vac1_vacsM",
            "vac1_vacs_any_percent_F",
            "vac1_vacs_any_percent_M",
            "vac2_vacsF",
            "vac2_vacsM",
            "vac2_vacs_any_percent_F",
            "vac2_vacs_any_percent_M",
            "populationin1000sF",
            "populationin1000sM"
        ],
        flatten = True
    )

    print("\n" + "=" * 30 + "\n")


    check(
        native_coll="disease_data",  # Your Native Collection Name
        opt_coll="disease_data",  # Your Optimized Collection Name
        unique_keys=["date"],
        numeric_cols=["cases", "deaths", "recovered"]
    )

    print("\n" + "=" * 30 + "\n")

    check(
        native_coll="vac_data",  # Your Native Collection Name
        opt_coll="vac_data",  # Your Optimized Collection Name
        unique_keys=["date"],
        numeric_cols=["daily_vaccinated", "total_vaccinated"]
    )

    print("\n" + "=" * 30 + "\n")

    check(
        native_coll="vaccinations_cleaned",  # Your Native Collection Name
        opt_coll="vaccinations_cleaned",  # Your Optimized Collection Name
        unique_keys=["location", "date"],
        numeric_cols=[
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
            "daily_vaccinations_raw",
            "daily_vaccinations",
            "total_vaccinations_per_hundred",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
            "total_boosters_per_hundred",
            "daily_vaccinations_per_million",
            "daily_people_vaccinated",
            "daily_people_vaccinated_per_hundred"
        ]
    )

    print("\n" + "=" * 30 + "\n")
    check(
        native_coll="summary_collection",  # Your Native Collection Name
        opt_coll="summary_collection",  # Your Optimized Collection Name
        unique_keys=["country", "date"],
        numeric_cols=[
            "population_total_in_thousands",
            "population_male_in_thousands",
            "population_female_in_thousands",
            "testing_male",
            "testing_female",
            "testing_male_percent",
            "testing_female_percent",
            "cases_total",
            "cases_total_sum_disaggregated",
            "cases_male",
            "cases_female",
            "cases_male_percent",
            "cases_female_percent",
            "deaths_total",
            "deaths_total_sum_disaggregated",
            "deaths_male",
            "deaths_female",
            "deaths_male_percent",
            "deaths_female_percent",
            "hospitalizations_total",
            "hospitalizations_male",
            "hospitalizations_female",
            "hospitalizations_male_percent",
            "hospitalizations_female_percent",
            "icu_admissions_total",
            "icu_admissions_male",
            "icu_admissions_female",
            "icu_admissions_male_percent",
            "icu_admissions_female_percent",
            "infected_healthcare_workers_total",
            "infected_healthcare_workers_male",
            "infected_healthcare_workers_female",
            "infected_healthcare_workers_male_percent",
            "infected_healthcare_workers_female_percent",
            "case_fatality_rate_total_percent",
            "case_fatality_rate_male_percent",
            "case_fatality_rate_female_percent",
            "case_fatality_rate_male_to_female_ratio",
            "vaccinations_first_dose_total",
            "vaccinations_first_dose_male",
            "vaccinations_first_dose_female",
            "vaccinations_first_dose_male_percent",
            "vaccinations_first_dose_female_percent",
            "vaccinations_second_dose_total",
            "vaccinations_second_dose_male",
            "vaccinations_second_dose_female",
            "vaccinations_second_dose_male_percent",
            "vaccinations_second_dose_female_percent"
        ],
        flatten=True
    )

    print("\n" + "=" * 30 + "\n")



if __name__ == "__main__":
    main()