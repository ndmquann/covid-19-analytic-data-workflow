import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import certifi
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
    """
    print(f"\n--- DATA OVERVIEW REPORT: {dataset_name.upper()} ---")
    print(f"\n1. Data Shape: {df.shape}")
    print("\n2. First 5 rows:")
    print(df.head())
    print("\n3. Data info:")
    df.info()
    print("\n4. Descriptive statistics:")
    print(df.describe().T)
    print(f"--- END OF OVERVIEW REPORT ---")

def clean_and_preprocess(df):
    """
    Performs data cleaning and preprocessing steps.
    """
    print("\n\n--- Starting data cleaning and preprocessing ---")

    print("1. Standardizing 'date' data type and sorting values...")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date'], inplace=True)
    df = df.sort_values(by=['location', 'date'])

    print("2. Handling duplicate data...")
    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        print(f"   - Found {num_duplicates} duplicate rows. Deleting...")
        df = df.drop_duplicates()
    else:
        print("   - No duplicate data found.")

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

    # Plot 1: Distribution
    plt.figure(figsize=(12, 6))
    sns.histplot(df[df['daily_vaccinations'] > 0]['daily_vaccinations'], bins=100, kde=True, log_scale=True)
    plt.title('Distribution of Daily Vaccinations (Log Scale)')

    # Plot 2: Top 10 Countries
    df_countries = df[~df['iso_code'].str.startswith('OWID_', na=False)]
    latest_data = df_countries.loc[df_countries.groupby('location')['date'].idxmax()]
    top_10 = latest_data.nlargest(10, 'people_vaccinated')

    plt.figure(figsize=(12, 8))
    sns.barplot(x='people_vaccinated', y='location', data=top_10, palette='viridis')
    plt.title('Top 10 Countries by Total People Vaccinated')

    # Plot 3: Heatmap
    plt.figure(figsize=(14, 10))
    numeric_df = df.select_dtypes(include=['float64', 'int64'])
    sns.heatmap(numeric_df.corr(), annot=False, cmap='coolwarm')
    plt.title('Correlation Heatmap')

    # Plot 4: Time Series
    global_vac_time = df.groupby('date')['daily_vaccinations'].sum().cumsum()
    plt.figure(figsize=(14, 7))
    global_vac_time.plot(kind='line', lw=2)
    plt.title('Total Global Vaccinations Over Time')
    plt.grid(True)

    plt.tight_layout()
    # Note: plt.show() blocks execution. Comment out if running automatically.
    plt.show()


def save_to_mongodb(df, connection_string, db_name, collection_name):
    """
    Saves data to MongoDB using batch processing.
    """
    print("\n--- Saving data to MongoDB ---")
    try:
        # Use certifi for secure SSL context
        client = MongoClient(connection_string, tlsCAFile=certifi.where())
        db = client[db_name]
        collection = db[collection_name]

        print(f"Clearing existing data in '{collection_name}'...")
        collection.delete_many({})

        print("Converting data to dictionary format...")
        records = df.to_dict(orient='records')
        total_records = len(records)
        batch_size = 5000

        print(f"Inserting {total_records} records in batches of {batch_size}...")
        for i in range(0, total_records, batch_size):
            batch = records[i: i + batch_size]
            collection.insert_many(batch, ordered=False)
            print(f"   Saved {min(i + batch_size, total_records)} / {total_records} records...")

        print("Successfully inserted all documents.")

        # Re-create index
        try:
            collection.drop_index("loc_date_idx")
        except Exception:
            pass  # Index didn't exist

        collection.create_index([("location", 1), ("date", -1)], name="loc_date_idx")
        client.close()

    except Exception as e:
        print(f"ERROR: Could not save to MongoDB. Error: {e}")


def main():
    """
    Main execution workflow.
    """
    input_filepath = 'vaccinations.csv'

    # Retrieve securely from environment variables
    # Create a .env file with: MONGO_URI=mongodb+srv://user:pass@...
    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = "testing_db"
    COLLECTION_NAME = "vaccinations_cleaned"

    if not MONGO_URI:
        print("Error: MONGO_URI not found in environment variables.")
        return

    df = load_data(input_filepath)

    if df is not None:
        summarize_data(df, dataset_name="original")
        df_cleaned = clean_and_preprocess(df)

        # Uncomment to view charts
        # analyze_and_visualize(df_cleaned)

        save_to_mongodb(df_cleaned, MONGO_URI, DB_NAME, COLLECTION_NAME)


if __name__ == '__main__':
    main()