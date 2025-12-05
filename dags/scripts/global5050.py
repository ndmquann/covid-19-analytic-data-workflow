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

global_hist_url ="https://api.global5050.org/api/v1/summary?data=historic"
global_hist_data = requests.get(global_hist_url).json()
global_hist_data = global_hist_data["data"]
print(json.dumps(global_hist_data, indent=3))
global_agesex_url = "https://api.global5050.org/api/v1/agesex"
global_agesex_data = requests.get(global_agesex_url).json()
global_agesex_data = global_agesex_data["data"]
# print(json.dumps(global_agesex_data, indent =3 ))
# Setup Mongodb


load_dotenv() # Load variables from .env file
mongo_uri = os.getenv("MONGO_URI") # Get the URI securely
client = MongoClient(mongo_uri)


db = client["testing_db"]

def global_agesex_retrive_vacs_country(data):
    country_name = []
    for country, categories in data.items():
        for typ, recordlst in categories.items():
            if typ == "VacsbyAgeSex":
                country_name.append(country)
    return country_name

def global_agesex_convert_date_time():
    def convert_date(attr_dict):
        attr_dict["date"] = pd.to_datetime(attr_dict["date"], format="mixed")
        return attr_dict
    data = global_agesex_data
    country = global_agesex_retrive_vacs_country(global_agesex_data)
    iter = dict(data)
    for country_name, categories in iter.items():
        if country_name in country:
            for typ, recordlst in categories.items():
                data[country_name][typ] = [convert_date(x) for x in recordlst]
        else:
            data.pop(country_name)
    return data

def global_summary_convert_date_time():
    def convert_date(attr_dict):
        for key, value in attr_dict.items():
            if "date" in key.lower():
                attr_dict[key] = pd.to_datetime(value, format='mixed')
                if attr_dict[key] is pd.NaT:
                  attr_dict[key] = ""
        return attr_dict
    data = global_hist_data
    iter = dict(data)
    for country_name, data_date_timeline_lst in iter.items():
        data[country_name] = [convert_date(x) for x in data_date_timeline_lst]
    return data

def flatten_data(data_dict):
    records = []
    for country_key, recs in data_dict.items():
        for rec in recs:
            rec_copy = rec.copy()
            records.append(rec_copy)
    return records
def flatten_agesex_data():
    records = []
    data = global_agesex_data
    count = 0
    for country, category in data.items():
        for typ, recordlst in category.items():
            for rec in recordlst:
                rec_copy = rec.copy()
                records.append(rec_copy)
                count += 1
    return count, records

def compute_missing_percent(records):
    df = pd.DataFrame(records)
    # Replace empty string "" with NA
    df = df.replace("", pd.NA)
    missing_frac = df.isna().mean()
    missing_pct = missing_frac * 100
    return missing_pct.sort_values(ascending=False), df

def print_missing_percent(data):
    missing_pct, df = compute_missing_percent(data)
    print("Missing % by attribute:")
    print(missing_pct.to_string())
    # show columns with very low missing (update frequently) and very high missing (update rarely)
    print("\nColumns with ≥ 90% missing:")
    print(missing_pct[missing_pct >= 90])
    print("\nColumns with ≤ 10% missing:")
    print(missing_pct[missing_pct <= 10])


def plot_missing_percent(data):
    missing_pct, df = compute_missing_percent(data)
    # plot
    plt.figure(figsize=(10, 6))
    plt.bar(missing_pct.index, missing_pct.values)
    plt.xticks(rotation=90)
    plt.ylabel("Missing Percentage (%)")
    plt.title("Missing % by Attribute")
    plt.tight_layout()
    plt.show()

    # optional: highlight high/low missing cols
    high_missing = missing_pct[missing_pct >= 90].index.tolist()
    low_missing = missing_pct[missing_pct <= 10].index.tolist()
    print("Columns with ≥90% missing:", high_missing)
    print("Columns with ≤10% missing:", low_missing)

def find_min_max_mean_sum(data):
    df = pd.DataFrame(data)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    min_max_mean_sum = pd.DataFrame({
        "min": df.min(numeric_only=True),
        "max": df.max(numeric_only=True),
        "mean": df.mean(numeric_only=True),
        "sum": df.sum(numeric_only=True)
    })
    pd.set_option("display.float_format", "{:,.2f}".format)
    print(min_max_mean_sum)


def format_summary_data(data):
    records = []
    data_schema = {
        "country": "String",
        "country_code": "String",
        "date": "datetime",
        "population": {
            "total_in_thousands": "float",
            "male_in_thousands": "float",
            "female_in_thousands": "float"
        },
        "testing": {
            "date": "datetime",
            "male": "integer",
            "female": "integer",
            "male_percent": "float",
            "female_percent": "float"
        },
        "cases": {
            "date": "datetime",
            "total": "integer",
            "total_sum_disaggregated": "integer",
            "male": "integer",
            "female": "integer",
            "male_percent": "float",
            "female_percent": "float"
        },
        "deaths": {
            "date": "datetime",
            "total": "integer",
            "total_sum_disaggregated": "integer",
            "male": "integer",
            "female": "integer",
            "male_percent": "float",
            "female_percent": "float"
        },
        "hospitalizations": {
            "date": "datetime",
            "total": "integer",
            "male": "integer",
            "female": "integer",
            "male_percent": "float",
            "female_percent": "float"
        },
        "icu_admissions": {
            "date": "datetime",
            "total": "integer",
            "male": "integer",
            "female": "integer",
            "male_percent": "float",
            "female_percent": "float"
        },
        "infected_healthcare_workers": {
            "date": "datetime",
            "total": "integer",
            "male": "integer",
            "female": "integer",
            "male_percent": "float",
            "female_percent": "float"
        },
        "case_fatality_rate": {
            "date": "datetime",
            "total_percent": "float",
            "male_percent": "float",
            "female_percent": "float",
            "male_to_female_ratio": "float"
        },
        "vaccinations": {
            "first_dose": {
                "date": "datetime",
                "total": "integer",
                "male": "integer",
                "female": "integer",
                "male_percent": "float",
                "female_percent": "float"
            },
            "second_dose": {
                "date": "datetime",
                "total": "integer",
                "male": "integer",
                "female": "integer",
                "male_percent": "float",
                "female_percent": "float"
            }
        }
    }
    for country_key, recs in data.items():
        for rec in recs:
            data_schema_cpy = copy.deepcopy(data_schema)
            data_schema_cpy["country"] = rec["country"]
            data_schema_cpy["country_code"] = rec["country_code"]
            data_schema_cpy["date"] = rec["date"]
            data_schema_cpy["population"]["total_in_thousands"] = rec["totpop2020"]
            data_schema_cpy["population"]["male_in_thousands"] = rec["malepop2020"]
            data_schema_cpy["population"]["female_in_thousands"] = rec["femalepop2020"]

            data_schema_cpy["testing"]["date"] = rec["tests_date"]
            data_schema_cpy["testing"]["male"] = rec["tests_male"]
            data_schema_cpy["testing"]["female"] = rec["tests_female"]
            data_schema_cpy["testing"]["male_percent"] = rec["tests_male_pct"]
            data_schema_cpy["testing"]["female_percent"] = rec["tests_female_pct"]

            data_schema_cpy["cases"]["date"] = rec["cases_date"]
            data_schema_cpy["cases"]["total"] = rec["cases_total"]
            data_schema_cpy["cases"]["total_sum_disaggregated"] = rec["cases_total_sum"]
            data_schema_cpy["cases"]["male"] = rec["cases_male"]
            data_schema_cpy["cases"]["female"] = rec["cases_female"]
            data_schema_cpy["cases"]["male_percent"] = rec["cases_male_pct"]
            data_schema_cpy["cases"]["female_percent"] = rec["cases_female_pct"]

            data_schema_cpy["deaths"]["date"] = rec["deaths_date"]
            data_schema_cpy["deaths"]["total"] = rec["deaths_total"]
            data_schema_cpy["deaths"]["total_sum_disaggregated"] = rec["deaths_total_sum"]
            data_schema_cpy["deaths"]["male"] = rec["deaths_male"]
            data_schema_cpy["deaths"]["female"] = rec["deaths_female"]
            data_schema_cpy["deaths"]["male_percent"] = rec["deaths_male_pct"]
            data_schema_cpy["deaths"]["female_percent"] = rec["deaths_female_pct"]

            data_schema_cpy["hospitalizations"]["date"] = rec["hosp_date"]
            data_schema_cpy["hospitalizations"]["total"] = rec["hosp_total"]
            data_schema_cpy["hospitalizations"]["male"] = rec["hosp_male"]
            data_schema_cpy["hospitalizations"]["female"] = rec["hosp_female"]
            data_schema_cpy["hospitalizations"]["male_percent"] = rec["hosp_male_pct"]
            data_schema_cpy["hospitalizations"]["female_percent"] = rec["hosp_female_pct"]

            data_schema_cpy["icu_admissions"]["date"] = rec["icu_date"]
            data_schema_cpy["icu_admissions"]["total"] = rec["icu_total"]
            data_schema_cpy["icu_admissions"]["male"] = rec["icu_male"]
            data_schema_cpy["icu_admissions"]["female"] = rec["icu_female"]
            data_schema_cpy["icu_admissions"]["male_percent"] = rec["icu_male_pct"]
            data_schema_cpy["icu_admissions"]["female_percent"] = rec["icu_female_pct"]

            data_schema_cpy["infected_healthcare_workers"]["date"] = rec["healthcare_date"]
            data_schema_cpy["infected_healthcare_workers"]["total"] = rec["healthcare_total"]
            data_schema_cpy["infected_healthcare_workers"]["male"] = rec["healthcare_male"]
            data_schema_cpy["infected_healthcare_workers"]["female"] = rec["healthcare_female"]
            data_schema_cpy["infected_healthcare_workers"]["male_percent"] = rec["healthcare_male_pct"]
            data_schema_cpy["infected_healthcare_workers"]["female_percent"] = rec["healthcare_female_pct"]

            data_schema_cpy["case_fatality_rate"]["date"] = rec["cfr_date"]
            data_schema_cpy["case_fatality_rate"]["total_percent"] = rec["cfr_pct_tot"]
            data_schema_cpy["case_fatality_rate"]["male_percent"] = rec["cfr_pct_male"]
            data_schema_cpy["case_fatality_rate"]["female_percent"] = rec["cfr_pct_female"]
            data_schema_cpy["case_fatality_rate"]["male_to_female_ratio"] = rec["cfr_ratio"]

            data_schema_cpy["vaccinations"]["first_dose"]["date"] = rec["vac1_date"]
            data_schema_cpy["vaccinations"]["first_dose"]["total"] = rec["vac1_total"]
            data_schema_cpy["vaccinations"]["first_dose"]["male"] = rec["vac1_male"]
            data_schema_cpy["vaccinations"]["first_dose"]["female"] = rec["vac1_female"]
            data_schema_cpy["vaccinations"]["first_dose"]["male_percent"] = rec["vac1_male_pct"]
            data_schema_cpy["vaccinations"]["first_dose"]["female_percent"] = rec["vac1_female_pct"]

            data_schema_cpy["vaccinations"]["second_dose"]["date"] = rec["vac2_date"]
            data_schema_cpy["vaccinations"]["second_dose"]["total"] = rec["vac2_total"]
            data_schema_cpy["vaccinations"]["second_dose"]["male"] = rec["vac2_male"]
            data_schema_cpy["vaccinations"]["second_dose"]["female"] = rec["vac2_female"]
            data_schema_cpy["vaccinations"]["second_dose"]["male_percent"] = rec["vac2_male_pct"]
            data_schema_cpy["vaccinations"]["second_dose"]["female_percent"] = rec["vac2_female_pct"]
            records.append(data_schema_cpy)
    return records

def fill_data(records):
    def fill_recursive(current, previous):
        """Recursively fill empty strings in current with values from previous."""
        if not isinstance(current, dict):
            # Fill missing leaf value
            if current == "" and previous != "":
                return previous
            else:
                return current

        filled = {}
        for key, value in current.items():
            prev_value = previous.get(key, "") if isinstance(previous, dict) else ""
            filled[key] = fill_recursive(value, prev_value)
        return filled

    if not records:
        return []

    filled_records = []
    prev_record = None
    prev_country = None

    for record in records:
        curr_country = record.get("country")

        # When country changes, reset previous record
        if curr_country != prev_country:
            prev_record = None
            prev_country = curr_country
            filled_records.append(record.copy())  # keep first record for each country unchanged
            prev_record = record
            continue

        # Fill missing values from previous record (same country)
        filled_record = fill_recursive(record, prev_record)
        filled_records.append(filled_record)
        prev_record = filled_record  # update previous

    return filled_records

def format_agesex_data(data):
    case_records = []
    death_records = []
    vac_records = []

    case_agesex_schema =  {
        "country": "string",
        "date": "datetime",
        "age_begin": "integer",
        "age_end": "integer",
        "casesF": "integer",
        "casesM": "integer",
        "populationin1000sF": "float",
        "populationin1000sM": "float"
    }
    death_agesex_schema = {
        "country": "string",
        "date": "datetime",
        "age_begin": "integer",
        "age_end": "integer",
        "deathsF": "integer",
        "deathsM": "integer",
        "populationin1000sF": "float",
        "populationin1000sM": "float"
    }
    vac_agesex_schema = {
        "country": "string",
        "date": "datetime",
        "age_begin": "integer",
        "age_end": "integer",
        "vac1": {
            "vacsF": "integer",
            "vacsM": "integer",
            "vacs_any_percent_F": "float",
            "vacs_any_percent_M": "float",
        },
        "vac2": {
            "vacsF": "integer",
            "vacsM": "integer",
            "vacs_any_percent_F": "float",
            "vacs_any_percent_M": "float",
        },
        "populationin1000sF": "float",
        "populationin1000sM": "float"
    }
    for country_name, categories in data.items():
        for typ, recordlst in categories.items():
            match typ:
                case "CasebyAgeSex":
                    for record in recordlst:

                        case_agesex_schema_copy = copy.deepcopy(case_agesex_schema)
                        case_agesex_schema_copy["country"] = record["country"]
                        case_agesex_schema_copy["date"] = record["date"]
                        case_agesex_schema_copy["age_begin"] = record["age_begin"]
                        case_agesex_schema_copy["age_end"] = record["age_end"]
                        case_agesex_schema_copy["casesF"] = record["casesF"]
                        case_agesex_schema_copy["casesM"] = record["casesM"]
                        case_agesex_schema_copy["populationin1000sF"] = record["populationin1000sF"]
                        case_agesex_schema_copy["populationin1000sM"] = record["populationin1000sM"]
                        case_records.append(case_agesex_schema_copy)
                case "DeathsbyAgeSex":
                    for record in recordlst:
                        death_agesex_schema_copy = copy.deepcopy(death_agesex_schema)
                        death_agesex_schema_copy["country"] = record["country"]
                        death_agesex_schema_copy["date"] = record["date"]
                        death_agesex_schema_copy["age_begin"] = record["age_begin"]
                        death_agesex_schema_copy["age_end"] = record["age_end"]
                        death_agesex_schema_copy["deathsF"] = record["deathsF"]
                        death_agesex_schema_copy["deathsM"] = record["deathsM"]
                        death_agesex_schema_copy["populationin1000sF"] = record["populationin1000sF"]
                        death_agesex_schema_copy["populationin1000sM"] = record["populationin1000sM"]
                        death_records.append(death_agesex_schema_copy)
                case "VacsbyAgeSex":
                    for record in recordlst:
                        vac_agesex_schema_copy = copy.deepcopy(vac_agesex_schema)
                        vac_agesex_schema_copy["country"] = record["country"]
                        vac_agesex_schema_copy["date"] = record["date"]
                        vac_agesex_schema_copy["age_begin"] = record["age_begin"]
                        vac_agesex_schema_copy["age_end"] = record["age_end"]
                        vac_agesex_schema_copy["populationin1000sF"] = record["populationin1000sF"]
                        vac_agesex_schema_copy["populationin1000sM"] = record["populationin1000sM"]
                        vac_agesex_schema_copy["vac1"]["vacsF"] = record["vacsF"]
                        vac_agesex_schema_copy["vac1"]["vacsM"] = record["vacsM"]
                        vac_agesex_schema_copy["vac1"]["vacs_any_percent_F"] = record["vacs_any_percent_F"]
                        vac_agesex_schema_copy["vac1"]["vacs_any_percent_M"] = record["vacs_any_percent_M"]
                        vac_agesex_schema_copy["vac2"]["vacsF"] = record["vacs2F"]
                        vac_agesex_schema_copy["vac2"]["vacsM"] = record["vacs2M"]
                        vac_agesex_schema_copy["vac2"]["vacs_any_percent_F"] = record["vacs_2_percent_F"]
                        vac_agesex_schema_copy["vac2"]["vacs_any_percent_M"] = record["vacs_2_percent_M"]
                        vac_records.append(vac_agesex_schema_copy)
    return case_records, death_records, vac_records

def load_global_data_into_db():
    #init collection
    summary_collection = db["summary_collection"]
    case_agesex_collection = db["case_agesex_collection"]
    death_agesex_collection = db["death_agesex_collection"]
    vac_agesex_collection = db["vac_agesex_collection"]
    summary_collection.delete_many({})
    case_agesex_collection.delete_many({})
    death_agesex_collection.delete_many({})
    vac_agesex_collection.delete_many({})
    # load summary data
    data = global_summary_convert_date_time()
    data = format_summary_data(data)
    data = fill_data(data)
    summary_collection.insert_many(data)
    # load agesex data
    data = global_agesex_convert_date_time()
    casedata, deathdata, vacdata = format_agesex_data(data)
    case_agesex_collection.insert_many(casedata)
    death_agesex_collection.insert_many(deathdata)
    vac_agesex_collection.insert_many(vacdata)

if __name__ == '__main__':
    load_global_data_into_db()
    # for doc in summary_collection.find({"country": "Vietnam"}):
    #     pprint(doc, sort_dicts=False)