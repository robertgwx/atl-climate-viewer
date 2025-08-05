import pandas as pd
import seaborn as sns
from io import StringIO
import requests
import re
import os
import sys
from datetime import datetime
from tabulate import tabulate
from scipy import stats
import glob
import concurrent.futures
from heapq import nlargest

# Assuming base_url is defined globally or passed to functions that need it
base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
valid_provinces = ['NL', 'NS', 'PEI', 'NB', 'QC']

## DEFINE ALL FUNCTIONS
# Function to convert title case, but avoid capitalizing letters after apostrophes.
def custom_title_case(text):
    return re.sub(r"(\s|^)([a-z])|(')([a-z])",
                  lambda m: (m.group(1) or "") + (m.group(2).upper() if m.group(2) else "")
                  if m.group(1) or m.group(2) else (m.group(3) or "") + (m.group(4) if m.group(4) else ""),
                  text)

def fetch_daily_data(station_id, station_name, year, current_month):
    all_daily_data = []
    current_date = datetime.now()

    # Fetch data for all months in the year
    for month in range(1, 12):
        # Skip months after the current month if it's the current year
        if year == current_date.year and month > current_date.month:
            break
        params = {
            "format": "csv",
            "stationID": station_id,
            "Year": year,
            "Month": month,
            "timeframe": 2,  # Daily data
        }

        try:
            response = requests.get(base_url, params=params)

            if response.status_code == 200 and response.content.strip():
                csv_data = StringIO(response.content.decode("utf-8"))
                data = pd.read_csv(csv_data)

                if not data.empty:
                    # Define desired columns
                    desired_columns = [
                        "Date/Time",
                        "Max Temp (°C)",
                        "Min Temp (°C)",
                        "Mean Temp (°C)",
                        "Total Rain (mm)",
                        "Total Snow (cm)",
                        "Total Precip (mm)",
                        "Snow on Grnd (cm)",
                        "Spd of Max Gust (km/h)"
                    ]

                    # Filter columns that actually exist in the dataframe
                    existing_columns = [col for col in desired_columns if col in data.columns]

                    # Select only existing columns
                    data = data[existing_columns]

                    # Filter data for specific month and year
                    data_month = data[
                        (pd.to_datetime(data['Date/Time']).dt.month == month) &
                        (pd.to_datetime(data['Date/Time']).dt.year == year)
                    ].copy()

                    # Format 'Date/Time' to only include date (YYYY-MM-DD)
                    data_month['Date/Time'] = pd.to_datetime(data_month['Date/Time']).dt.strftime('%Y-%m-%d')

                    # Add station info to daily data
                    data_month['Station Name'] = station_name
                    data_month['Station ID'] = station_id
                    all_daily_data.extend(data_month.to_dict('records'))

        except Exception as e:
            print(f"Error fetching data for {station_name} in {year}, month {month}: {e}")

    return all_daily_data

def remove_duplicate_dates(filepath):
    """
    Removes duplicate dates from a single CSV file, keeping the row with more data.
    """
    columns_to_check = ['Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)', 'Total Precip (mm)',
                        'Total Rain (mm)', 'Total Snow (cm)', 'Snow on Grnd (cm)', 'Spd of Max Gust (km/h)']
    try:
        df = pd.read_csv(filepath)
        df['Date/Time'] = pd.to_datetime(df['Date/Time'])
        df = df.sort_values(by=['Date/Time'])

        # Group by date and select the best row in each group
        df = df.loc[df.groupby('Date/Time')[columns_to_check].apply(
            lambda x: x.notnull().sum(axis=1).idxmax()
        )]

        df.to_csv(filepath, index=False)

    except Exception as e:
        print(f"Error processing {filepath}: {e}")

# New function to update a single CSV file with the most recent data
def update_csv_file(file_path):
    columns_to_check = ['Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)', 'Total Precip (mm)',
                        'Total Rain (mm)', 'Total Snow (cm)', 'Snow on Grnd (cm)', 'Spd of Max Gust (km/h)']
    try:
        # Read existing data
        existing_df = pd.read_csv(file_path)

        if existing_df.empty:
            print(f"File {file_path} is empty. Skipping.")
            return

         # --- START: Added code to clean Station ID ---
        if 'Station ID' in existing_df.columns:
            # Convert to string, remove any decimals, and convert to integer
            existing_df['Station ID'] = existing_df['Station ID'].astype(str).str.replace(r'\.0$', '', regex=True)
            # Attempt to convert to integer, handle potential errors
            try:
                 existing_df['Station ID'] = existing_df['Station ID'].astype(int)
            except ValueError:
                 print("Warning: Could not convert all Station IDs to integers after removing decimals.", file=sys.stdout)
                 # If conversion to int fails, keep them as strings to avoid crashing
        # --- END: Added code to clean Station ID ---

        # Get the most recent Station ID and name from existing data
        most_recent_station_id = existing_df['Station ID'].iloc[-1]
        station_name = existing_df['Station Name'].iloc[-1]

        # Get the most recent date from the CSV
        most_recent_date = pd.to_datetime(existing_df['Date/Time']).max()
        most_recent_year = most_recent_date.year
        most_recent_month = most_recent_date.month
        most_recent_day = most_recent_date.day

        # Get current year and month
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        current_day = current_date.day

        print(f"  Most recent data is from: {most_recent_date.strftime('%Y-%m-%d')}")
        print(f"  Current date: {current_date.strftime('%Y-%m-%d')}")

        # Check if the data needs updating (if most recent date is not yesterday or today)
        one_day_ago = current_date.replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=1)

        if most_recent_date >= one_day_ago:
            print(f"  Data is already up to date for {station_name} (data goes up to {most_recent_date.strftime('%Y-%m-%d')}). No update needed.")
            return

        # Define the years and months we need to fetch
        months_to_fetch = []

        # Figure out what months we need to fetch
        # If we're still in the same month as most_recent_date, just fetch this month
        if most_recent_year == current_year and most_recent_month == current_month:
            months_to_fetch.append((current_year, current_month))
        else:
            # Otherwise, fetch all months from most_recent_month to current_month
            fetch_year = most_recent_year
            fetch_month = most_recent_month

            # Keep adding months until we reach the current month
            while (fetch_year < current_year) or (fetch_year == current_year and fetch_month <= current_month):
                months_to_fetch.append((fetch_year, fetch_month))
                fetch_month += 1
                if fetch_month > 12:
                    fetch_month = 1
                    fetch_year += 1

        print(f"  Fetching data for {len(months_to_fetch)} months...")

        # Fetch new data for each month
        all_new_records = []
        for year, month in months_to_fetch:
            new_records = fetch_daily_data(most_recent_station_id, station_name, year, month)
            all_new_records.extend(new_records)

        if all_new_records:
            # Convert new records to DataFrame
            new_df = pd.DataFrame(all_new_records)

            # Remove rows where ALL specified columns are empty/NaN
            new_df = new_df.dropna(subset=columns_to_check, how='all')

            # Filter out dates that are already in the existing data
            existing_dates = pd.to_datetime(existing_df['Date/Time'])
            new_df['Date/Time'] = pd.to_datetime(new_df['Date/Time'])
            new_df = new_df[~new_df['Date/Time'].isin(existing_dates)]

            # Format 'Date/Time' to only include date (YYYY-MM-DD)
            new_df['Date/Time'] = pd.to_datetime(new_df['Date/Time']).dt.strftime('%Y-%m-%d')

            if not new_df.empty:
                # Combine existing data with new data
                df_comprehensive_data = pd.concat([existing_df, new_df], ignore_index=True)

                # Save the data to the CSV file
                df_comprehensive_data.to_csv(file_path, mode="w", index=False)

                print(f"  Updated {station_name} with {len(new_df)} new records.")

                # Remove duplicate dates
                remove_duplicate_dates(file_path)

                print(f"  Duplicate data removed for {station_name}.")

            else:
                print(f"  No new data found after filtering existing dates.")
        else:
            print(f"  No new data found.")

    except Exception as e:
        print(f"  Error updating {file_path}: {str(e)}")

def main():
    # New code for updating all CSV files
    base_dir = "climate_data"

    if not os.path.exists(base_dir):
        print(f"Error: Directory {base_dir} not found.")
        return

     # Find all CSV files in all province subdirectories
    all_csv_files = []

    for province in valid_provinces:
        province_dir = os.path.join(base_dir, province)
        if os.path.exists(province_dir):
            csv_pattern = os.path.join(province_dir, "*.csv")
            province_csv_files = glob.glob(csv_pattern)
            all_csv_files.extend(province_csv_files)

    # Deduplicate file paths
    all_csv_files = list(set(all_csv_files))

    if not all_csv_files:
        print("No CSV files found in any province directories.")
        return

    print(f"\nFound {len(all_csv_files)} CSV files to update.")

    # Use ThreadPoolExecutor to update files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for i, file_path in enumerate(all_csv_files, 1):
            print(f"\nQueueing file {i}/{len(all_csv_files)}: {os.path.basename(file_path)}")
            futures.append(executor.submit(update_csv_file, file_path))
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("\nAll files have been updated with the most recent data.")

if __name__ == "__main__":
    main()
