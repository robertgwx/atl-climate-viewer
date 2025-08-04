import os
import pandas as pd

def remove_duplicate_dates_single_file(filepath, columns_to_check):
    """
    Reads a single CSV file, removes duplicate dates keeping the row with more data,
    and saves the processed data back to the same file.
    """
    try:
        df = pd.read_csv(filepath)
        df['Date/Time'] = pd.to_datetime(df['Date/Time'])
        df = df.sort_values(by=['Date/Time'])

        # Group by date and select the best row in each group
        df = df.loc[df.groupby('Date/Time')[columns_to_check].apply(
            lambda x: x.notnull().sum(axis=1).idxmax()
        )]

        df.to_csv(filepath, index=False)
        print(f"Processed: {filepath}")

    except Exception as e:
        print(f"Error processing {filepath}: {e}")


# Specify the folder path
province = input("Enter the province abbreviation (e.g., NL, NS): ").upper()
folder_path = f'climate_data/{province}'

# Columns to check for emptiness
columns_to_check = ['Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)', 'Total Precip (mm)',
                    'Total Rain (mm)', 'Total Snow (cm)', 'Snow on Grnd (cm)', 'Spd of Max Gust (km/h)']

# Check if the folder exists
if not os.path.exists(folder_path):
    print(f"Error: Directory not found at {folder_path}")
else:
    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            remove_duplicate_dates_single_file(filepath, columns_to_check)
    print("Processing complete.")