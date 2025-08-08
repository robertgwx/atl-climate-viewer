import pandas as pd
import os
from datetime import datetime
import requests
from io import StringIO

def fetch_single_day_data(station_id, year, month, day, base_url):
    """
    Fetches data for a single specific day from the climate data source.

    Args:
        station_id (str): Unique identifier for the weather station.
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
        base_url (str): The base URL for fetching climate data.

    Returns:
        pd.DataFrame or None: DataFrame with the single day's data, or None if fetching fails or no data.
    """
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

            if not data.empty and 'Date/Time' in data.columns:
                 # Ensure 'Date/Time' is in datetime format for filtering
                data['Date/Time'] = pd.to_datetime(data['Date/Time'], errors='coerce')

                # Filter for the specific day
                single_day_data = data[
                    (data['Date/Time'].dt.year == year) &
                    (data['Date/Time'].dt.month == month) &
                    (data['Date/Time'].dt.day == day)
                ].copy()

                if not single_day_data.empty:
                     # Convert 'Date/Time' back to string format for consistency if needed later, or keep as datetime
                    # For this purpose, keeping as datetime might be better for merging
                    # single_day_data['Date/Time'] = single_day_data['Date/Time'].dt.strftime('%Y-%m-%d')
                    return single_day_data[['Date/Time', 'Dir of Max Gust (10s deg)']] # Return only relevant columns
                else:
                    return None
            else:
                return None

    except Exception as e:
        print(f"  Error fetching data for station {station_id} on {year}-{month}-{day}: {e}")
        return None


def fill_missing_gust_direction(folder_path, base_url):
    """
    Iterates through CSV files, finds missing 'Dir of Max Gust (10s deg)' values,
    and attempts to fetch and fill them using the associated station ID and date.

    Args:
        folder_path (str): The path to the folder containing the CSV files.
        base_url (str): The base URL for fetching climate data.
    """
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                csv_file_path = os.path.join(root, file)
                try:
                    print(f"Processing: {csv_file_path}")
                    df = pd.read_csv(csv_file_path)

                    # Ensure required columns exist
                    if 'Date/Time' not in df.columns or 'Station ID' not in df.columns or 'Dir of Max Gust (10s deg)' not in df.columns:
                        print(f"  Required columns ('Date/Time', 'Station ID', 'Dir of Max Gust (10s deg)') not found in {file}. Skipping.")
                        continue

                    # Ensure 'Date/Time' is in datetime format
                    df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')

                    # Identify rows with missing 'Dir of Max Gust (10s deg)'
                    missing_mask = df['Dir of Max Gust (10s deg)'].isna()

                    if missing_mask.sum() == 0:
                        print(f"  No missing 'Dir of Max Gust (10s deg)' values in {file}.")
                        continue

                    print(f"  Found {missing_mask.sum()} missing 'Dir of Max Gust (10s deg)' values.")

                    # Iterate through rows with missing data
                    for index, row in df[missing_mask].iterrows():
                        date_to_fetch = row['Date/Time']
                        station_id_to_fetch = row['Station ID']

                        if pd.isna(date_to_fetch) or pd.isna(station_id_to_fetch):
                            print(f"  Skipping row {index} due to missing Date/Time or Station ID.")
                            continue

                        # Fetch data for the specific day and station
                        single_day_df = fetch_single_day_data(
                            station_id_to_fetch,
                            date_to_fetch.year,
                            date_to_fetch.month,
                            date_to_fetch.day,
                            base_url
                        )

                        if single_day_df is not None and not single_day_df.empty:
                            # Update the missing value in the original DataFrame
                            # Ensure the date formats match for indexing
                            fetched_date = pd.to_datetime(single_day_df['Date/Time'].iloc[0])
                            fetched_direction = single_day_df['Dir of Max Gust (10s deg)'].iloc[0]

                            if pd.notna(fetched_direction):
                                # Find the row in the original DataFrame that matches the fetched date
                                # Using a small tolerance for datetime comparison might be necessary
                                # Alternatively, convert both to a standard string format for matching
                                matching_row_index = df[df['Date/Time'] == fetched_date].index

                                if not matching_row_index.empty:
                                     # Update the 'Dir of Max Gust (10s deg)' in the original DataFrame at the found index
                                    df.loc[matching_row_index[0], 'Dir of Max Gust (10s deg)'] = fetched_direction
                                    print(f"  Filled missing value for {fetched_date.strftime('%Y-%m-%d')} with {fetched_direction}.")
                                else:
                                     print(f"  Could not find matching row for date {fetched_date.strftime('%Y-%m-%d')} in original DataFrame.")

                        # Add a small delay to avoid overwhelming the server
                        import time
                        time.sleep(0.1)


                    # Convert 'Dir of Max Gust (10s deg)' to numeric and multiply by 10 for all rows
                    df['Dir of Max Gust (10s deg)'] = pd.to_numeric(df['Dir of Max Gust (10s deg)'], errors='coerce')
                    df['Dir of Max Gust (10s deg)'] = df['Dir of Max Gust (10s deg)'] * 10

                    # Convert 'Date/Time' back to string format for saving
                    df['Date/Time'] = df['Date/Time'].dt.strftime('%Y-%m-%d')

                    # Save the updated DataFrame back to the CSV file
                    df.to_csv(csv_file_path, index=False)
                    print(f"  Finished processing and saved {file}.")


                except Exception as e:
                    print(f"Error processing {file}: {e}")

    print("\nGust direction data fill complete.")

# Set the folder path and base URL
folder_path = 'climate_data/PEI'
base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html" # Ensure this is the correct base URL

# Run the update function
fill_missing_gust_direction(folder_path, base_url)
