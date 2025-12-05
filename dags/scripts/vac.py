import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pymongo import MongoClient, ASCENDING, DESCENDING, GEOSPHERE
from datetime import datetime
from pprint import pprint
import os
from dotenv import load_dotenv
# --- FUNCTION DEFINITIONS ---

def load_data(filepath):
    """
    Loads data from a CSV file and returns a DataFrame.
    """
    print(f"Loading data from: {filepath}...")
    if not os.path.exists(filepath):
        print(f"ERROR: File not found at path '{filepath}'")
        return None

    df = pd.read_csv(filepath)
    print("Data loaded successfully!")
    return df

def summarize_data(df, dataset_name=""):
    """
    Prints a summary overview of the DataFrame.
    `dataset_name` is used to describe the data (e.g., 'original', 'cleaned').
    """
    print(f"\n--- DATA OVERVIEW REPORT: {dataset_name.upper()} ---")

    # 1. Data Shape
    print(f"\n1. Data Shape:")
    print(f"   - Rows: {df.shape[0]}")
    print(f"   - Columns: {df.shape[1]}")

    # 2. First 5 rows
    print("\n2. First 5 rows (.head()):")
    print(df.head())

    # 3. Data info and types
    print("\n3. Data info and types (.info()):")
    df.info()

    # 4. Descriptive statistics for numerical columns
    print("\n4. Descriptive statistics for numerical columns (.describe()):")
    print(df.describe().T)

    print(f"--- END OF OVERVIEW REPORT ---")


def clean_and_preprocess(df):
    """
    Performs data cleaning and preprocessing steps.
    """
    print("\n\n--- Starting data cleaning and preprocessing ---")

    # 1. Standardize 'date' data type and sort values
    print("1. Standardizing 'date' data type and sorting values...")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # FIX: Drop rows with invalid dates (NaT) to prevent errors with idxmax()
    df.dropna(subset=['date'], inplace=True)

    df = df.sort_values(by=['location', 'date'])

    # 2. Handle duplicate data
    print("2. Handling duplicate data...")
    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        print(f"   - Found {num_duplicates} duplicate rows. Deleting...")
        df = df.drop_duplicates()
    else:
        print("   - No duplicate data found.")

    # 3. Handle missing data
    print("3. Handling missing data...")
    print("   - Applying 'forward-fill' method per location group.")
    df = df.groupby('location', group_keys=False).apply(lambda group: group.ffill())

    print("   - Filling remaining missing values with 0.")
    df.fillna(0, inplace=True)

    print("Preprocessing complete!")
    return df

def analyze_and_visualize(df):
    """
    Performs data analysis and visualization.
    """
    print("\n\n--- Starting analysis and visualization ---")

    # --- Plot 1: Distribution of Daily Vaccinations ---
    plt.figure(figsize=(12, 6))
    sns.histplot(df[df['daily_vaccinations'] > 0]['daily_vaccinations'], bins=100, kde=True, log_scale=True)
    plt.title('Distribution of Daily Vaccinations (Log Scale)', fontsize=16)
    plt.xlabel('Daily Vaccinations', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)

    # --- Plot 2: Top 10 Countries by Total People Vaccinated ---
    df_countries_only = df[~df['iso_code'].str.startswith('OWID_', na=False)]
    latest_data = df_countries_only.loc[df_countries_only.groupby('location')['date'].idxmax()]
    top_10_countries = latest_data.nlargest(10, 'people_vaccinated')

    plt.figure(figsize=(12, 8))
    sns.barplot(x='people_vaccinated', y='location', data=top_10_countries, palette='viridis')
    plt.title('Top 10 Countries by Total People Vaccinated', fontsize=16)
    plt.xlabel('Total People Vaccinated', fontsize=12)
    plt.ylabel('Country', fontsize=12)

    # --- Plot 3: Correlation Heatmap ---
    df_numeric = df.select_dtypes(include=['float64', 'int64'])
    correlation_matrix = df_numeric.corr()

    plt.figure(figsize=(14, 10))
    sns.heatmap(correlation_matrix, annot=False, cmap='coolwarm')
    plt.title('Correlation Heatmap of Numerical Features', fontsize=16)

    # --- Plot 4: Total Global Vaccinations Over Time ---
    global_vaccinations_over_time = df.groupby('date')['daily_vaccinations'].sum().cumsum()

    plt.figure(figsize=(14, 7))
    global_vaccinations_over_time.plot(kind='line', lw=2)
    plt.title('Total Global Vaccinations Over Time', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Vaccinations (in Billions)', fontsize=12)
    plt.grid(True)

    print("Analysis and visualization complete! Please close the plot windows to exit.")
    plt.tight_layout()
    plt.show()


from pymongo import InsertOne


def save_to_mongodb(df, connection_string, db_name, collection_name):
    """
    Saves data to MongoDB with performance optimizations (Batching + Ordered=False).
    """
    print("\n--- Saving data to MongoDB (Optimized) ---")
    try:
        # 1. Connect
        import certifi
        client = MongoClient(connection_string, tlsCAFile=certifi.where())
        db = client[db_name]
        collection = db[collection_name]

        # 2. Clear existing data
        print(f"Clearing existing data in '{collection_name}'...")
        collection.delete_many({})

        # 3. Convert DataFrame to dicts
        print("Converting data to dictionary format...")
        records = df.to_dict(orient='records')
        total_records = len(records)

        # 4. Insert in Batches
        batch_size = 5000  # Send 5,000 records per network request
        print(f"Inserting {total_records} records in batches of {batch_size}...")

        # Loop through data in chunks
        for i in range(0, total_records, batch_size):
            batch = records[i: i + batch_size]

            # ordered=False allows MongoDB to insert in parallel and not stop on errors
            collection.insert_many(batch, ordered=False)

            # Print progress
            print(f"   Saved {min(i + batch_size, total_records)} / {total_records} records...")

        print("Successfully inserted all documents.")
        # Create indexing
        try:
            collection.drop_index("loc_date_idx")
        except Exception as e:
            print(f"Not found: {e}")
        collection.create_index(
            [("location", 1), ("date", -1)],
            name="loc_date_idx"
        )
        client.close()

    except Exception as e:
        print(f"ERROR: Could not save to MongoDB. Error: {e}")


# --- MAIN EXECUTION BLOCK ---
def save_cleaned_data(df, output_path):
    """
    Saves the cleaned DataFrame to a new CSV file.
    """
    try:
        output_dir = os.path.dirname(output_path)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df.to_csv(output_path, index=False)
        print(f"\nCleaned data saved successfully to: {output_path}")
    except Exception as e:
        print(f"ERROR: Could not save the file. Error: {e}")

# --- MAIN EXECUTION BLOCK ---

def main():
    """
    The main function to orchestrate the entire workflow.
    """
    # File Configuration
    input_filepath = 'vaccinations.csv'

    # MongoDB Configuration
    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = "testing_db"
    COLLECTION_NAME = "vaccinations_cleaned"

    # 1. Load data
    df = load_data(input_filepath)

    if df is not None:
        print("Columns found:", df.columns.tolist())
        # 2. Overview of the original data
        summarize_data(df, dataset_name="original")

        # 3. Clean and preprocess data
        df_cleaned = clean_and_preprocess(df)
        print("\nColumns RIGHT BEFORE analysis:", df_cleaned.columns.tolist())

        # 4. Analyze and visualize data
        # analyze_and_visualize(df_cleaned)

        # 5. Save the cleaned data to MongoDB
        save_to_mongodb(df_cleaned, MONGO_URI, DB_NAME, COLLECTION_NAME)


if __name__ == '__main__':
    main()