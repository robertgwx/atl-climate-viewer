import pandas as pd
import os
from datetime import datetime
import requests
from io import StringIO
import time
from tqdm import tqdm  # Add tqdm for progress bars

def fetch_single_day_data(station_id, year, month, day, base_url, cache):
    key = (station_id, year, month, day)
    if key in cache:
        return cache[key]
    params = {
        "format": "csv",
        "stationID": station_id,
        "Year": year,
        "Month": month,
        "timeframe": 2,
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200 and response.content.strip():
            csv_data = StringIO(response.content.decode("utf-8"))
            data = pd.read_csv(csv_data)
            if not data.empty and 'Date/Time' in data.columns:
                data['Date/Time'] = pd.to_datetime(data['Date/Time'], errors='coerce')
                single_day_data = data[
                    (data['Date/Time'].dt.year == year) &
                    (data['Date/Time'].dt.month == month) &
                    (data['Date/Time'].dt.day == day)
                ].copy()
                cache[key] = single_day_data[['Date/Time', 'Dir of Max Gust (10s deg)']]
                return cache[key]
    except Exception as e:
        print(f"  Error fetching data for station {station_id} on {year}-{month}-{day}: {e}")
    cache[key] = None
    return None

def fill_missing_gust_direction(folder_path, base_url):
    # Gather all CSV files first for tqdm
    all_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                all_files.append(os.path.join(root, file))
    print(f"Found {len(all_files)} CSV files to process.")

    for csv_file_path in tqdm(all_files, desc="Processing files"):
        file = os.path.basename(csv_file_path)
        try:
            df = pd.read_csv(csv_file_path)
            # Ensure required columns exist
            if 'Date/Time' not in df.columns or 'Station ID' not in df.columns:
                print(f"  Required columns not found in {file}. Skipping.")
                continue
            if 'Dir of Max Gust (10s deg)' not in df.columns:
                df['Dir of Max Gust (10s deg)'] = pd.NA
            df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')
            missing_mask = df['Dir of Max Gust (10s deg)'].isna()
            if missing_mask.sum() == 0:
                continue
            print(f"  {file}: Found {missing_mask.sum()} missing values.")
            # Collect unique (station, date) pairs
            missing_rows = df[missing_mask][['Station ID', 'Date/Time']]
            unique_requests = missing_rows.dropna().drop_duplicates()
            cache = {}
            fetched_data = []
            for _, row in tqdm(unique_requests.iterrows(), total=unique_requests.shape[0], desc=f"Fetching data for {file}", leave=False):
                station_id = row['Station ID']
                date = row['Date/Time']
                if pd.isna(station_id) or pd.isna(date):
                    continue
                result = fetch_single_day_data(
                    station_id, date.year, date.month, date.day, base_url, cache
                )
                if result is not None and not result.empty:
                    fetched_data.append(result)
                time.sleep(0.05)
            # Merge all fetched data
            if fetched_data:
                merged = pd.concat(fetched_data)
                merged['Date/Time'] = pd.to_datetime(merged['Date/Time'], errors='coerce')
                df = df.merge(
                    merged,
                    on='Date/Time',
                    how='left',
                    suffixes=('', '_fetched')
                )
                # Fill missing values
                df['Dir of Max Gust (10s deg)'] = df['Dir of Max Gust (10s deg)'].combine_first(df['Dir of Max Gust (10s deg)_fetched'])
                df.drop(columns=['Dir of Max Gust (10s deg)_fetched'], inplace=True)
            # Convert and save
            df['Dir of Max Gust (10s deg)'] = pd.to_numeric(df['Dir of Max Gust (10s deg)'], errors='coerce') * 10
            df['Date/Time'] = df['Date/Time'].dt.strftime('%Y-%m-%d')
            df.to_csv(csv_file_path, index=False)
        except Exception as e:
            print(f"Error processing {file}: {e}")
    print("\nGust direction data fill complete.")

folder_path = 'climate_data/PEI'
base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
fill_missing_gust_direction(folder_path, base_url)