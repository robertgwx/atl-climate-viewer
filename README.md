# Climate Data Dashboard

A dynamic web dashboard for visualizing daily climate data across Atlantic Canada provinces (NB, NL, NS, PEI).

![Dashboard Screenshot](index.html)

## Features

- Interactive charts for temperature and precipitation
- Province and location selection
- Responsive design (Bootstrap 5)
- Data sourced from CSV files in `climate_data/`

## Project Structure

```
fetch_and_update_data.py        # Script to fetch and update climate data
generate_locations_json.py      # Script to generate locations.json from CSVs
index.html                      # Main dashboard web page
locations.json                  # Location metadata for the dashboard
climate_data/                   # Climate data CSVs organized by province
    NB/
    NL/
    NS/
    PEI/
```

## Getting Started

1. **Clone the repository:**
   ```sh
   git clone <your-repo-url>
   cd upgraded-spoon
   ```

2. **Update or fetch data (optional):**
   ```sh
   python3 fetch_and_update_data.py
   ```

3. **Generate `locations.json`:**
   ```sh
   python3 generate_locations_json.py
   ```

4. **Open the dashboard:**
   ```sh
   $BROWSER index.html
   ```

## Dependencies

- Python 3.x (for data scripts)
- [Bootstrap 5](https://getbootstrap.com/)
- [PapaParse](https://www.papaparse.com/) (CSV parsing)
- [Chart.js](https://www.chartjs.org/) (charts)

## Data Sources

Climate data CSVs are organized by province in the `climate_data/` directory.

## License

MIT License

---

*Made with ❤️ for Atlantic Canada climate data exploration.*