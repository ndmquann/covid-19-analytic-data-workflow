import os
import copy
import requests
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import certifi
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


# --- API Fetching ---
def fetch_data():
    try:
        print("Fetching global summary...")
        global_hist_url = "https://api.global5050.org/api/v1/summary?data=historic"
        summary_resp = requests.get(global_hist_url).json()

        print("Fetching age/sex data...")
        global_agesex_url = "https://api.global5050.org/api/v1/agesex"
        agesex_resp = requests.get(global_agesex_url).json()

        return summary_resp["data"], agesex_resp["data"]
    except Exception as e:
        print(f"Error fetching API data: {e}")
        return None, None


# --- Data Processing Helpers ---

def global_agesex_convert_date_time(data):
    """Converts date strings to datetime objects in the nested dictionary."""
    # Note: We filter for 'VacsbyAgeSex' existence to identify valid countries
    valid_countries = []
    for country, categories in data.items():
        if "VacsbyAgeSex" in categories:
            valid_countries.append(country)

    processed_data = {}
    for country, categories in data.items():
        if country in valid_countries:
            processed_data[country] = {}
            for typ, record_list in categories.items():
                new_list = []
                for rec in record_list:
                    rec['date'] = pd.to_datetime(rec['date'], format="mixed")
                    new_list.append(rec)
                processed_data[country][typ] = new_list

    return processed_data


def global_summary_convert_date_time(data):
    """Converts date strings to datetime objects in the summary data."""
    processed_data = copy.deepcopy(data)
    for country, records in processed_data.items():
        for rec in records:
            for key, value in rec.items():
                if "date" in key.lower() and value:
                    rec[key] = pd.to_datetime(value, format='mixed', errors='coerce')
                    if pd.isna(rec[key]):
                        rec[key] = ""
    return processed_data


def fill_data_optimized(records):
    """
    Optimized filling of missing values using Pandas ffill.
    Replaces the previous recursive dictionary approach.
    """
    if not records:
        return []

    df = pd.DataFrame(records)

    # Forward fill missing values within each country group
    # We first replace empty strings with NaN for ffill to work
    df = df.replace("", np.nan)
    df = df.groupby("country", group_keys=False).apply(lambda x: x.ffill())

    # Fill remaining NaNs back to empty strings or defaults if needed,
    # but keeping as None/NaN is often better for DBs.
    # Here we revert to empty string to match original logic.
    df = df.fillna("")

    return df.to_dict(orient='records')


def format_summary_data(data):
    """
    Maps the raw API data to the desired schema structure.
    """
    records = []

    # Base schema structure for reference
    base_schema = {
        "country": "", "country_code": "", "date": None,
        "population": {}, "testing": {}, "cases": {}, "deaths": {},
        "hospitalizations": {}, "icu_admissions": {},
        "infected_healthcare_workers": {}, "case_fatality_rate": {},
        "vaccinations": {"first_dose": {}, "second_dose": {}}
    }

    for country_key, recs in data.items():
        for rec in recs:
            # Deep copy to ensure a fresh structure for every record
            item = copy.deepcopy(base_schema)

            item["country"] = rec.get("country")
            item["country_code"] = rec.get("country_code")
            item["date"] = rec.get("date")

            item["population"] = {
                "total_in_thousands": rec.get("totpop2020"),
                "male_in_thousands": rec.get("malepop2020"),
                "female_in_thousands": rec.get("femalepop2020")
            }

            item["testing"] = {
                "date": rec.get("tests_date"),
                "male": rec.get("tests_male"),
                "female": rec.get("tests_female"),
                "male_percent": rec.get("tests_male_pct"),
                "female_percent": rec.get("tests_female_pct")
            }

            item["cases"] = {
                "date": rec.get("cases_date"),
                "total": rec.get("cases_total"),
                "total_sum_disaggregated": rec.get("cases_total_sum"),
                "male": rec.get("cases_male"),
                "female": rec.get("cases_female"),
                "male_percent": rec.get("cases_male_pct"),
                "female_percent": rec.get("cases_female_pct")
            }

            item["deaths"] = {
                "date": rec.get("deaths_date"),
                "total": rec.get("deaths_total"),
                "total_sum_disaggregated": rec.get("deaths_total_sum"),
                "male": rec.get("deaths_male"),
                "female": rec.get("deaths_female"),
                "male_percent": rec.get("deaths_male_pct"),
                "female_percent": rec.get("deaths_female_pct")
            }

            item["hospitalizations"] = {
                "date": rec.get("hosp_date"),
                "total": rec.get("hosp_total"),
                "male": rec.get("hosp_male"),
                "female": rec.get("hosp_female"),
                "male_percent": rec.get("hosp_male_pct"),
                "female_percent": rec.get("hosp_female_pct")
            }

            item["icu_admissions"] = {
                "date": rec.get("icu_date"),
                "total": rec.get("icu_total"),
                "male": rec.get("icu_male"),
                "female": rec.get("icu_female"),
                "male_percent": rec.get("icu_male_pct"),
                "female_percent": rec.get("icu_female_pct")
            }

            item["infected_healthcare_workers"] = {
                "date": rec.get("healthcare_date"),
                "total": rec.get("healthcare_total"),
                "male": rec.get("healthcare_male"),
                "female": rec.get("healthcare_female"),
                "male_percent": rec.get("healthcare_male_pct"),
                "female_percent": rec.get("healthcare_female_pct")
            }

            item["case_fatality_rate"] = {
                "date": rec.get("cfr_date"),
                "total_percent": rec.get("cfr_pct_tot"),
                "male_percent": rec.get("cfr_pct_male"),
                "female_percent": rec.get("cfr_pct_female"),
                "male_to_female_ratio": rec.get("cfr_ratio")
            }

            item["vaccinations"]["first_dose"] = {
                "date": rec.get("vac1_date"),
                "total": rec.get("vac1_total"),
                "male": rec.get("vac1_male"),
                "female": rec.get("vac1_female"),
                "male_percent": rec.get("vac1_male_pct"),
                "female_percent": rec.get("vac1_female_pct")
            }

            item["vaccinations"]["second_dose"] = {
                "date": rec.get("vac2_date"),
                "total": rec.get("vac2_total"),
                "male": rec.get("vac2_male"),
                "female": rec.get("vac2_female"),
                "male_percent": rec.get("vac2_male_pct"),
                "female_percent": rec.get("vac2_female_pct")
            }

            records.append(item)
    return records


def format_agesex_data(data):
    """Formatted Age/Sex data for Cases, Deaths, and Vaccinations."""
    case_records = []
    death_records = []
    vac_records = []

    for country_name, categories in data.items():
        for typ, recordlst in categories.items():
            if typ == "CasebyAgeSex":
                for record in recordlst:
                    case_records.append({
                        "country": record.get("country"),
                        "date": record.get("date"),
                        "age_begin": record.get("age_begin"),
                        "age_end": record.get("age_end"),
                        "casesF": record.get("casesF"),
                        "casesM": record.get("casesM"),
                        "populationin1000sF": record.get("populationin1000sF"),
                        "populationin1000sM": record.get("populationin1000sM")
                    })
            elif typ == "DeathsbyAgeSex":
                for record in recordlst:
                    death_records.append({
                        "country": record.get("country"),
                        "date": record.get("date"),
                        "age_begin": record.get("age_begin"),
                        "age_end": record.get("age_end"),
                        "deathsF": record.get("deathsF"),
                        "deathsM": record.get("deathsM"),
                        "populationin1000sF": record.get("populationin1000sF"),
                        "populationin1000sM": record.get("populationin1000sM")
                    })
            elif typ == "VacsbyAgeSex":
                for record in recordlst:
                    vac_records.append({
                        "country": record.get("country"),
                        "date": record.get("date"),
                        "age_begin": record.get("age_begin"),
                        "age_end": record.get("age_end"),
                        "populationin1000sF": record.get("populationin1000sF"),
                        "populationin1000sM": record.get("populationin1000sM"),
                        "vac1": {
                            "vacsF": record.get("vacsF"),
                            "vacsM": record.get("vacsM"),
                            "vacs_any_percent_F": record.get("vacs_any_percent_F"),
                            "vacs_any_percent_M": record.get("vacs_any_percent_M"),
                        },
                        "vac2": {
                            "vacsF": record.get("vacs2F"),
                            "vacsM": record.get("vacs2M"),
                            "vacs_any_percent_F": record.get("vacs_2_percent_F"),
                            "vacs_any_percent_M": record.get("vacs_2_percent_M"),
                        }
                    })
    return case_records, death_records, vac_records


def load_global_data_into_db(summary_data, agesex_data):
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        print("Error: MONGO_URI not set.")
        return

    client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
    db = client["testing_db"]

    # Collections
    summary_collection = db["summary_collection"]
    case_agesex_collection = db["case_agesex_collection"]
    death_agesex_collection = db["death_agesex_collection"]
    vac_agesex_collection = db["vac_agesex_collection"]

    print("Clearing collections...")
    summary_collection.delete_many({})
    case_agesex_collection.delete_many({})
    death_agesex_collection.delete_many({})
    vac_agesex_collection.delete_many({})

    print("Processing and Loading Summary Data...")
    processed_summary = global_summary_convert_date_time(summary_data)
    formatted_summary = format_summary_data(processed_summary)

    # Using the optimized pandas filling instead of recursion
    filled_summary = fill_data_optimized(formatted_summary)

    if filled_summary:
        summary_collection.insert_many(filled_summary)

    print("Processing and Loading Age/Sex Data...")
    processed_agesex = global_agesex_convert_date_time(agesex_data)
    casedata, deathdata, vacdata = format_agesex_data(processed_agesex)

    if casedata: case_agesex_collection.insert_many(casedata)
    if deathdata: death_agesex_collection.insert_many(deathdata)
    if vacdata: vac_agesex_collection.insert_many(vacdata)

    print("Data load complete.")
    client.close()


def main():
    summary_data, agesex_data = fetch_data()
    if summary_data and agesex_data:
        load_global_data_into_db(summary_data, agesex_data)


if __name__ == '__main__':
    main()