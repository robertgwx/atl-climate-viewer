import os
import re
import pandas as pd
import requests
import json
from io import StringIO
from datetime import datetime, timedelta

# --- SETTINGS ---
STATION_CSV = "Station Inventory EN.csv"
LOCATIONS_JSON = "locations.json"
DATA_DIR = "climate_data"
BASE_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"

WEATHER_COLUMNS = [
    "Max Temp (°C)", "Min Temp (°C)", "Mean Temp (°C)",
    "Total Rain (mm)", "Total Snow (cm)", "Total Precip (mm)",
    "Snow on Grnd (cm)", "Spd of Max Gust (km/h)"
]
COLUMNS = ["Date/Time"] + WEATHER_COLUMNS

# --- YOUR EXACT CUSTOM TITLE CASE FUNCTION ---
def custom_title_case(text):
    return re.sub(
        r"(\s|^)([a-z])|(')([a-z])",
        lambda m: (m.group(1) or "") + (m.group(2).upper() if m.group(2) else "")
            if m.group(1) or m.group(2) else (m.group(3) or "") + (m.group(4) if m.group(4) else ""),
        text
    )

def fetch_daily_csv(station_id, station_name, year, month):
    params = {"format":"csv", "stationID":station_id, "Year":year, "Month":month, "timeframe":2}
    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    if r.content.strip():
        df = pd.read_csv(StringIO(r.content.decode("utf-8")))
        # Filter and format as notebook logic
        existing_columns = [col for col in COLUMNS if col in df.columns]
        df = df[existing_columns]
        # Collapsed/consistent date format
        df["Date/Time"] = pd.to_datetime(df["Date/Time"], errors="coerce").dt.strftime('%Y-%m-%d')
        df["Station Name"] = station_name
        df["Station ID"] = station_id
        return df.dropna(subset=["Date/Time"])
    return pd.DataFrame()

def remove_duplicates(df):
    if df.empty: return df
    # Use only columns in data
    real_cols = [col for col in WEATHER_COLUMNS if col in df.columns]
    df['non_null_score'] = df[real_cols].notna().sum(axis=1)
    df = df.sort_values(["Date/Time", "non_null_score"], ascending=[True, False])
    df = df.drop_duplicates(subset=["Date/Time"], keep='first')
    df = df.drop(columns=["non_null_score"])
    return df

def parse_province_abbr(province_full):
    mapping = {"NEWFOUNDLAND":"NL", "NOVA SCOTIA":"NS", "NEW BRUNSWICK":"NB",
               "QUEBEC":"QC", "PRINCE EDWARD ISLAND":"PEI"}
    pf = province_full.upper()
    return mapping.get(pf, pf[:2])

def main():
    # 1. Read locations.json — EVERY label gets custom_title_case for the match
    with open(LOCATIONS_JSON, "r", encoding="utf-8") as f:
        loc_json = json.load(f)
    label_set = set()
    for prov, prov_locs in loc_json.items():
        for entry in prov_locs:
            label_set.add(custom_title_case(entry['label']))

    print("Updating dashboard locations (labels):\n", sorted(label_set))

    # 2. Read station inventory
    inv = pd.read_csv(STATION_CSV, encoding="utf-8", dtype=str)
    inv = inv.rename(columns=lambda x: x.strip())
    now = datetime.now()
    updated_count = 0

    for idx, row in inv.iterrows():
        loc_raw = str(row["Name"]).strip()
        label = custom_title_case(loc_raw)
        if label not in label_set:
            continue

        province_full = str(row["Province"]).strip()
        station_id = str(row["Station ID"]).strip()
        first_year = int(row.get('DLY First Year', row.get('First Year', '1900')))
        last_year = int(row.get('DLY Last Year', row.get('Last Year', '2100')))
        if not station_id or first_year > last_year:
            continue

        province_abbr = parse_province_abbr(province_full)
        # For filesystem: underscores for spaces, but preserve everything else
        file_stub = label.replace(" ", "_")
        out_folder = os.path.join(DATA_DIR, province_abbr)
        os.makedirs(out_folder, exist_ok=True)
        out_file = os.path.join(out_folder, f"{file_stub}_daily_data.csv")

        print(f"\nUpdating: {label} ({province_abbr}), Station: {station_id}")

        # INCREMENTAL LOGIC
        existing = None
        from_year, from_month = first_year, 1

        if os.path.exists(out_file):
            try:
                current = pd.read_csv(out_file)
                if not current.empty and "Date/Time" in current.columns:
                    dates = pd.to_datetime(current["Date/Time"], errors="coerce")
                    last_dt = dates.dropna().max()
                    if not pd.isnull(last_dt):
                        next_day = last_dt + timedelta(days=1)
                        from_year = next_day.year
                        from_month = next_day.month
                        existing = current
                        print(f"Existing file found. Will only fetch {from_year}-{from_month:02d} onward.")
            except Exception as e:
                print("Failed to read existing file, will fetch all.")

        last_fetch_year = min(last_year, now.year)
        all_months = []
        for year in range(from_year, last_fetch_year + 1):
            m_start = from_month if year == from_year else 1
            m_end = now.month if year == now.year else 12
            for month in range(m_start, m_end + 1):
                print(f"  Fetching {year}-{month:02d}: ", end="")
                try:
                    df = fetch_daily_csv(station_id, label, year, month)
                    if not df.empty:
                        all_months.append(df)
                        print("OK", end="; ")
                    else:
                        print("NoData", end="; ")
                except Exception as e:
                    print(f"FAIL ({e})", end="; ")
            print()
        if all_months:
            fetched = pd.concat(all_months, ignore_index=True)
            fetched = fetched.dropna(how="all", subset=WEATHER_COLUMNS)
            if existing is not None and not existing.empty:
                combined = pd.concat([existing, fetched], ignore_index=True)
            else:
                combined = fetched
            clean = remove_duplicates(combined)
            clean.to_csv(out_file, index=False)
            updated_count += 1
            print(f">>> {out_file} updated: now {len(clean)} rows")
        else:
            if existing is not None and os.path.exists(out_file):
                print("> No new data; file unchanged.")
            else:
                print("(No data, skipped).")
    print(f"\nDone: {updated_count} dashboard locations updated.")

if __name__ == "__main__":
    main()
