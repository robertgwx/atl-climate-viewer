import os
import json

base_folder = "climate_data"
manifest = {}

for province in sorted(os.listdir(base_folder)):
    province_path = os.path.join(base_folder, province)
    if os.path.isdir(province_path):
        locations = []
        for fname in sorted(os.listdir(province_path)):
            if fname.endswith('_daily_data.csv'):
                # Create the "label" by stripping suffix and replacing _ with spaces
                label = fname.replace('_daily_data.csv', '').replace('_', ' ')
                # If the location is like "StJohns", add punctuation if you wish (optional)
                locations.append({
                    "label": label,
                    "file": f"{base_folder}/{province}/{fname}"
                })
        if locations:
            manifest[province] = locations

# Write manifest to JSON file
with open("locations.json", "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)
print("locations.json generated with", sum(len(l) for l in manifest.values()), "locations")
