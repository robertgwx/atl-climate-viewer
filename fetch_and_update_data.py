import os
import re
import pandas as pd
import requests
import json
from io import StringIO
from datetime import datetime

# -------- SETTINGS --------
STATION_CSV = "Station Inventory EN.csv"
LOCATIONS_JSON = "locations.json"
DATA_DIR = "Climate Data"
BASE_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"

WEATHER_COLUMNS = [
    "Max Temp (°C)", "Min Temp (°C)", "Mean Temp (°C)",
    "Total Rain (mm)", "Total Snow (cm)", "Total Precip (mm)",
    "Snow on Grnd (cm)", "Spd of Max Gust (km/h)"
]
COLUMNS = ["Date/Time"] + WEATHER_COLUMNS

def normalize_location_name(name):
    # Remove punctuation (dot, apostrophe, etc.), whitespaces, lowercase
    return re.sub(r"[.'’\s]", "", str(name)).lower()

def fetch_daily_csv(station_id, year, month):
    params = {"format":"csv", "stationID":station_id, "Year":year, "Month":month, "timeframe":2}
    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    if r.content.strip():
        return pd.read_csv(StringIO(r.content.decode("utf-8")))
    return pd.DataFrame()

def remove_duplicates(df):
    df = df[df["Date/Time"].notnull()].copy()
    df['Date/Time'] = pd.to_datetime(df['Date/Time'], errors='coerce')
    real_cols = [col for col in WEATHER_COLUMNS if col in df.columns]
    df['non_null_score'] = df[real_cols].notna().sum(axis=1)
    df = df.sort_values(["Date/Time", "non_null_score"], ascending=[True, False])
    no_dupes = df.drop_duplicates(subset=["Date/Time"], keep='first')
    no_dupes = no_dupes.drop(columns=["non_null_score"])
    no_dupes['Date/Time'] = no_dupes['Date/Time'].dt.strftime('%Y-%m-%d')
    return no_dupes

def main():
    # 1. Load location list from locations.json, normalize labels
    with open(LOCATIONS_JSON, 'r', encoding='utf-8') as f:
        loc_json = json.load(f)
    target_locations = set()
    for prov_locs in loc_json.values():
        for item in prov_locs:
            target_locations.add(normalize_location_name(item['label']))
    print(f"Dashboard locations to update (from locations.json):")
    print(target_locations)

    # 2. Load Station Inventory
    inv = pd.read_csv(STATION_CSV, encoding="utf-8", dtype=str)
    inv = inv.rename(columns=lambda x: x.strip())
    now = datetime.now()
    updated_count = 0

    for idx, row in inv.iterrows():
        location = str(row['Name']).strip()
        province_full = str(row['Province']).strip()
        station_id = str(row['Station ID']).strip()
        # Use DLY First/Last if present, else fallback to overall First/Last
        first_year = int(row.get('DLY First Year', row.get('First Year', '1900')))
        last_year = int(row.get('DLY Last Year', row.get('Last Year', '2100')))
        if not station_id or first_year > last_year:
            continue

        # Only update if this location is in our dashboard list
        if normalize_location_name(location) not in target_locations:
            continue

        province_abbr = None
        match = re.match(r"([A-Z]{2,4})", province_full)
        if match:
            province_abbr = match.group(1)
        else:
            mapping = {"NEWFOUNDLAND":"NL", "NOVA SCOTIA":"NS", "NEW BRUNSWICK":"NB",
                       "QUEBEC":"QC", "PRINCE EDWARD ISLAND":"PEI"}
            province_abbr = mapping.get(province_full.upper(), province_full[:2].upper())

        # Output file
        loc_name_clean = re.sub(r'[ .\'’]', '', location)
        out_folder = os.path.join(DATA_DIR, province_abbr)
        os.makedirs(out_folder, exist_ok=True)
        out_file = os.path.join(out_folder, f'{loc_name_clean}_daily_data.csv')

        print(f"Updating: {location} ({province_abbr}), Station: {station_id}")

        all_months = []
        for year in range(first_year, min(last_year, now.year)+1):
            max_month = now.month if year == now.year else 12
            for month in range(1, max_month+1):
                print(f"  {year}-{month:02d}: ", end="")
                try:
                    df = fetch_daily_csv(station_id, year, month)
                    if not df.empty and "Date/Time" in df.columns:
                        use_cols = [c for c in COLUMNS if c in df.columns]
                        df = df[use_cols]
                        df["Station Name"] = location
                        df["Station ID"] = station_id
                        all_months.append(df)
                        print(f"OK", end="; ")
                    else:
                        print("NoData", end="; ")
                except Exception as e:
                    print(f"FAIL ({e})", end="; ")
            print()
        if all_months:
            all_data = pd.concat(all_months, ignore_index=True)
            all_data = all_data.dropna(how='all', subset=WEATHER_COLUMNS)
            clean = remove_duplicates(all_data)
            clean.to_csv(out_file, index=False)
            updated_count += 1
            print(f">>> {out_file} saved ({len(clean)} rows)")
        else:
            print("(No data, skipped).")
    print(f"\nDone: {updated_count} dashboard locations updated.")

if __name__ == "__main__":
    main()