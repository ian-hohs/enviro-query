"""
download_data.py
================
Downloads real California environmental data from public government APIs.

Sources:
  - Air Quality : EPA AQS pre-generated daily AQI files (aqs.epa.gov)
  - Wildfires   : CAL FIRE / CNRA open data portal (data.cnra.ca.gov)
  - Water Quality: USGS Water Quality Portal (waterqualitydata.us)

Run once before launching the app:
    python data/download_data.py

Requires: requests, pandas  (both already in requirements.txt)
"""

import io
import os
import zipfile
import requests
import pandas as pd

OUT = os.path.join(os.path.dirname(__file__))


# ── Helpers ────────────────────────────────────────────────────────────────────

def save(df: pd.DataFrame, filename: str):
    path = os.path.join(OUT, filename)
    df.to_csv(path, index=False)
    print(f"  Saved {len(df):,} rows → {filename}")


# ── 1. AIR QUALITY — EPA AQS Daily AQI by County ──────────────────────────────
# Pre-generated ZIP files, one per year. Free, no API key required.
# Columns: State Name, county Name, State Code, County Code, Date, AQI,
#          Category, Defining Parameter, Defining Site, Number of Sites Reporting

def download_air_quality(years=(2021, 2022, 2023, 2024)):
    print("\n[1/3] Downloading air quality data from EPA AQS...")
    frames = []
    for year in years:
        url = f"https://aqs.epa.gov/aqsweb/airdata/daily_aqi_by_county_{year}.zip"
        print(f"  Fetching {year}...", end=" ", flush=True)
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                name = z.namelist()[0]
                df = pd.read_csv(z.open(name))
            # Filter California only
            df = df[df["State Name"] == "California"].copy()
            frames.append(df)
            print(f"{len(df):,} CA rows")
        except Exception as e:
            print(f"FAILED ({e})")

    if not frames:
        print("  No air quality data downloaded.")
        return

    raw = pd.concat(frames, ignore_index=True)

    # Rename and select columns to match app schema
    air = pd.DataFrame({
        "date":         raw["Date"],
        "county":       raw["county Name"].str.replace(" County", "", regex=False),
        "aqi":          pd.to_numeric(raw["AQI"], errors="coerce"),
        "aqi_category": raw["Category"],
        "defining_pollutant": raw["Defining Parameter"],
        "year":         pd.to_datetime(raw["Date"]).dt.year,
        "month":        pd.to_datetime(raw["Date"]).dt.month,
    }).dropna(subset=["aqi"])

    air["aqi"] = air["aqi"].astype(int)
    air = air.sort_values("date").reset_index(drop=True)
    save(air, "air_quality.csv")


# ── 2. WILDFIRES — CAL FIRE historical perimeters via CNRA open data ───────────
# CNRA ArcGIS REST API — returns JSON, no key required.
# Dataset: California Fire Perimeters (all) — updated annually.

def download_wildfires():
    print("\n[2/3] Downloading wildfire data from CAL FIRE / CNRA...")

    # Socrata-style query via ArcGIS FeatureServer
    base = (
        "https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/"
        "California_Fire_Perimeters/FeatureServer/0/query"
    )
    params = {
        "where": "YEAR_ >= 2015",
        "outFields": "YEAR_,ALARM_DATE,CONT_DATE,CAUSE,GIS_ACRES,COUNTY,FIRE_NAME,STRUCT_DESTROYED_TOTAL",
        "f": "json",
        "resultRecordCount": 5000,
        "orderByFields": "ALARM_DATE DESC",
    }

    print("  Fetching CAL FIRE perimeters...", end=" ", flush=True)
    try:
        r = requests.get(base, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        if not features:
            raise ValueError("No features returned")

        rows = [f["attributes"] for f in features]
        raw = pd.DataFrame(rows)
        print(f"{len(raw):,} records")

        # Parse epoch milliseconds → date strings
        def epoch_to_date(col):
            return pd.to_datetime(raw[col], unit="ms", errors="coerce").dt.strftime("%Y-%m-%d")

        fires = pd.DataFrame({
            "date_started":          epoch_to_date("ALARM_DATE"),
            "date_contained":        epoch_to_date("CONT_DATE"),
            "fire_name":             raw["FIRE_NAME"].str.title(),
            "county":                raw["COUNTY"].str.title(),
            "acres_burned":          pd.to_numeric(raw["GIS_ACRES"], errors="coerce").round(1),
            "cause":                 raw["CAUSE"].fillna("Unknown"),
            "structures_destroyed":  pd.to_numeric(raw["STRUCT_DESTROYED_TOTAL"], errors="coerce").fillna(0).astype(int),
            "year":                  pd.to_numeric(raw["YEAR_"], errors="coerce"),
        }).dropna(subset=["date_started", "acres_burned"])

        # Map numeric cause codes to labels
        cause_map = {
            "1": "Lightning", "2": "Equipment Use", "3": "Smoking",
            "4": "Campfire", "5": "Debris Burning", "6": "Railroad",
            "7": "Arson", "8": "Playing with Fire", "9": "Miscellaneous",
            "10": "Vehicle", "11": "Powerline", "12": "Firefighter Training",
            "13": "Non-Firefighter Training", "14": "Unknown",
            "15": "Structure", "16": "Aircraft", "17": "Escaped Prescribed Burn",
            "18": "Illegal Alien Campfire",
        }
        fires["cause"] = fires["cause"].astype(str).map(cause_map).fillna("Unknown")
        fires["month"] = pd.to_datetime(fires["date_started"], errors="coerce").dt.month
        fires = fires.sort_values("date_started", ascending=False).reset_index(drop=True)
        save(fires, "wildfires.csv")

    except Exception as e:
        print(f"\n  FAILED: {e}")
        print("  Falling back to synthetic wildfire data...")
        _synthetic_wildfires()


def _synthetic_wildfires():
    """Fallback if CAL FIRE API is unavailable."""
    import random
    from datetime import date, timedelta
    random.seed(99)
    COUNTIES = ["Los Angeles","San Francisco","Sacramento","San Diego","Fresno",
                "Alameda","Santa Clara","Riverside","San Bernardino","Kern",
                "Monterey","Sonoma","Napa","Marin","Shasta"]
    CAUSES = ["Lightning","Human","Equipment Use","Debris Burning","Unknown","Arson"]
    rows = []
    start, end = date(2015, 1, 1), date(2024, 12, 31)
    for _ in range(800):
        d = start + timedelta(days=random.randint(0, (end - start).days))
        acres = round(random.uniform(10, 9000) if d.month in [7,8,9,10] else random.uniform(1,500), 1)
        rows.append({
            "date_started": d.isoformat(),
            "date_contained": None,
            "fire_name": "Unknown",
            "county": random.choice(COUNTIES),
            "acres_burned": acres,
            "cause": random.choice(CAUSES),
            "structures_destroyed": random.randint(0, max(1, int(acres/50))),
            "year": d.year,
            "month": d.month,
        })
    save(pd.DataFrame(rows).sort_values("date_started", ascending=False), "wildfires.csv")


# ── 3. WATER QUALITY — USGS Water Quality Portal ──────────────────────────────
# WQP aggregates EPA, USGS, and state agency water quality data.
# REST API, no key required. Filters to California surface water measurements.

def download_water_quality():
    print("\n[3/3] Downloading water quality data from USGS Water Quality Portal...")

    url = "https://www.waterqualitydata.us/data/Result/search"
    params = {
        "statecode":        "US:06",        # California FIPS
        "characteristicName": ["pH",
                               "Turbidity",
                               "Dissolved oxygen (DO)",
                               "Nitrate"],
        "startDateLo":      "2018-01-01",
        "startDateHi":      "2024-12-31",
        "sampleMedia":      "Water",
        "dataProfile":      "narrowResult",
        "mimeType":         "csv",
        "zip":              "no",
    }

    print("  Fetching USGS WQP (this may take 30-60 seconds)...", end=" ", flush=True)
    try:
        r = requests.get(url, params=params, timeout=120, stream=True)
        r.raise_for_status()

        raw = pd.read_csv(io.StringIO(r.text), low_memory=False)
        print(f"{len(raw):,} raw records")

        # Keep only needed columns
        raw = raw[[
            "ActivityStartDate",
            "MonitoringLocationIdentifier",
            "CharacteristicName",
            "ResultMeasureValue",
            "ResultMeasure/MeasureUnitCode",
            "HydrologicEvent",
        ]].copy()

        raw["ResultMeasureValue"] = pd.to_numeric(raw["ResultMeasureValue"], errors="coerce")
        raw = raw.dropna(subset=["ResultMeasureValue", "ActivityStartDate"])

        # Pivot wide: one row per location+date, columns per parameter
        raw["date"] = pd.to_datetime(raw["ActivityStartDate"], errors="coerce").dt.strftime("%Y-%m-%d")
        raw["param"] = raw["CharacteristicName"].map({
            "pH": "ph",
            "Turbidity": "turbidity_ntu",
            "Dissolved oxygen (DO)": "dissolved_oxygen_mgl",
            "Nitrate": "nitrate_mgl",
        })
        raw = raw.dropna(subset=["param"])

        pivoted = (
            raw.groupby(["date", "MonitoringLocationIdentifier", "param"])["ResultMeasureValue"]
            .median()
            .unstack("param")
            .reset_index()
        )

        # Extract county from location identifier where possible
        # Location IDs often look like "USGS-11xxxxxx" — we'll leave as site ID
        # and use a county lookup based on known USGS site prefixes
        pivoted = pivoted.rename(columns={"MonitoringLocationIdentifier": "site_id"})

        # Add computed columns
        if "ph" not in pivoted.columns: pivoted["ph"] = None
        if "turbidity_ntu" not in pivoted.columns: pivoted["turbidity_ntu"] = None
        if "dissolved_oxygen_mgl" not in pivoted.columns: pivoted["dissolved_oxygen_mgl"] = None
        if "nitrate_mgl" not in pivoted.columns: pivoted["nitrate_mgl"] = None

        pivoted["meets_epa_standard"] = (
            pivoted["ph"].between(6.5, 8.5, inclusive="both") &
            (pivoted["turbidity_ntu"] <= 5.0) &
            (pivoted["nitrate_mgl"] <= 10.0)
        )
        pivoted["year"]  = pd.to_datetime(pivoted["date"], errors="coerce").dt.year
        pivoted["month"] = pd.to_datetime(pivoted["date"], errors="coerce").dt.month

        water = pivoted[[
            "date", "site_id", "ph", "turbidity_ntu",
            "dissolved_oxygen_mgl", "nitrate_mgl",
            "meets_epa_standard", "year", "month"
        ]].dropna(subset=["ph", "turbidity_ntu", "dissolved_oxygen_mgl"], how="all")

        water = water.sort_values("date").reset_index(drop=True)
        save(water, "water_quality.csv")

        # Update app schema note
        print("  Note: water_quality.csv uses 'site_id' instead of 'county'.")
        print("  The app schema comment in app.py should be updated to reflect this.")

    except Exception as e:
        print(f"\n  FAILED: {e}")
        print("  Falling back to synthetic water quality data...")
        _synthetic_water()


def _synthetic_water():
    """Fallback if USGS WQP is unavailable."""
    import random
    from datetime import date, timedelta
    random.seed(42)
    COUNTIES = ["Los Angeles","San Francisco","Sacramento","San Diego","Fresno",
                "Alameda","Santa Clara","Riverside","San Bernardino","Kern",
                "Monterey","Sonoma","Napa","Marin","Shasta"]
    SOURCES = ["Reservoir","River","Groundwater","Lake","Stream"]
    rows = []
    start, end = date(2018, 1, 1), date(2024, 12, 31)
    for _ in range(1000):
        d = start + timedelta(days=random.randint(0, (end - start).days))
        ph = round(random.uniform(6.2, 8.8), 2)
        turb = round(random.uniform(0.1, 12.0), 2)
        do_ = round(random.uniform(5.0, 12.0), 2)
        nit = round(random.uniform(0.1, 15.0), 2)
        rows.append({
            "date": d.isoformat(),
            "county": random.choice(COUNTIES),
            "water_source_type": random.choice(SOURCES),
            "ph": ph, "turbidity_ntu": turb,
            "dissolved_oxygen_mgl": do_, "nitrate_mgl": nit,
            "meets_epa_standard": ph >= 6.5 and ph <= 8.5 and turb <= 5.0 and nit <= 10.0,
            "year": d.year, "month": d.month,
        })
    save(pd.DataFrame(rows).sort_values("date"), "water_quality.csv")


# ── Run all ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  EnviroQuery — Real Data Downloader")
    print("  Sources: EPA AQS · CAL FIRE CNRA · USGS WQP")
    print("=" * 55)

    download_air_quality()
    download_wildfires()
    download_water_quality()

    print("\nDone. Run the app with: streamlit run app.py")
