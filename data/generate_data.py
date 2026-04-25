"""
Generate synthetic California environmental dataset for EnviroQuery.
Run this once to create the CSV files used by the app.
"""

import pandas as pd
import random
from datetime import date, timedelta

random.seed(42)

COUNTIES = [
    "Los Angeles", "San Francisco", "Sacramento", "San Diego", "Fresno",
    "Alameda", "Santa Clara", "Riverside", "San Bernardino", "Kern",
    "Monterey", "Sonoma", "Napa", "Marin", "Shasta"
]

CITIES = {
    "Los Angeles": ["Los Angeles", "Long Beach", "Pasadena"],
    "San Francisco": ["San Francisco", "Daly City"],
    "Sacramento": ["Sacramento", "Elk Grove"],
    "San Diego": ["San Diego", "Chula Vista"],
    "Fresno": ["Fresno", "Clovis"],
    "Alameda": ["Oakland", "Berkeley", "Fremont"],
    "Santa Clara": ["San Jose", "Sunnyvale", "Santa Clara"],
    "Riverside": ["Riverside", "Palm Springs"],
    "San Bernardino": ["San Bernardino", "Ontario"],
    "Kern": ["Bakersfield", "Ridgecrest"],
    "Monterey": ["Monterey", "Salinas"],
    "Sonoma": ["Santa Rosa", "Petaluma"],
    "Napa": ["Napa"],
    "Marin": ["San Rafael", "Mill Valley"],
    "Shasta": ["Redding"]
}

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# --- Air Quality Table ---
air_rows = []
start = date(2020, 1, 1)
end = date(2024, 12, 31)

for _ in range(2000):
    county = random.choice(COUNTIES)
    city = random.choice(CITIES[county])
    d = random_date(start, end)
    season = d.month
    # Fresno/Kern/LA have worse air quality
    base_aqi = 80 if county in ["Fresno", "Kern", "Los Angeles", "San Bernardino"] else 45
    # Summer ozone, winter particulates
    if season in [6, 7, 8, 9]:
        pm25 = round(random.uniform(5, 35), 1)
        ozone = round(random.uniform(0.040, 0.090), 3)
    else:
        pm25 = round(random.uniform(8, 55), 1)
        ozone = round(random.uniform(0.020, 0.055), 3)
    aqi = int(min(500, base_aqi + pm25 * 1.5 + random.randint(-15, 25)))
    category = (
        "Good" if aqi <= 50 else
        "Moderate" if aqi <= 100 else
        "Unhealthy for Sensitive Groups" if aqi <= 150 else
        "Unhealthy" if aqi <= 200 else "Very Unhealthy"
    )
    air_rows.append({
        "date": d.isoformat(),
        "county": county,
        "city": city,
        "pm25_ugm3": pm25,
        "ozone_ppm": ozone,
        "aqi": aqi,
        "aqi_category": category,
        "year": d.year,
        "month": d.month
    })

air_df = pd.DataFrame(air_rows).sort_values("date")

# --- Wildfire Table ---
fire_rows = []
CAUSES = ["Lightning", "Human", "Equipment Use", "Debris Burning", "Unknown", "Arson"]
for _ in range(600):
    county = random.choice(COUNTIES)
    d = random_date(start, end)
    # More fires in dry months
    if d.month in [7, 8, 9, 10]:
        acres = round(random.uniform(1, 8000), 1)
    else:
        acres = round(random.uniform(1, 500), 1)
    duration_days = max(1, int(acres / 200) + random.randint(0, 5))
    fire_rows.append({
        "date_started": d.isoformat(),
        "county": county,
        "acres_burned": acres,
        "duration_days": duration_days,
        "cause": random.choice(CAUSES),
        "structures_threatened": random.randint(0, int(acres / 10)),
        "structures_destroyed": random.randint(0, max(1, int(acres / 50))),
        "year": d.year,
        "month": d.month
    })

fire_df = pd.DataFrame(fire_rows).sort_values("date_started")

# --- Water Quality Table ---
WATER_SOURCES = ["Reservoir", "River", "Groundwater", "Lake", "Stream"]
water_rows = []
for _ in range(800):
    county = random.choice(COUNTIES)
    d = random_date(start, end)
    source = random.choice(WATER_SOURCES)
    ph = round(random.uniform(6.2, 8.8), 2)
    turbidity = round(random.uniform(0.1, 12.0), 2)
    dissolved_oxygen = round(random.uniform(5.0, 12.0), 2)
    nitrate = round(random.uniform(0.1, 15.0), 2)
    meets_standard = ph >= 6.5 and ph <= 8.5 and turbidity <= 5.0 and nitrate <= 10.0
    water_rows.append({
        "date": d.isoformat(),
        "county": county,
        "water_source_type": source,
        "ph": ph,
        "turbidity_ntu": turbidity,
        "dissolved_oxygen_mgl": dissolved_oxygen,
        "nitrate_mgl": nitrate,
        "meets_epa_standard": meets_standard,
        "year": d.year,
        "month": d.month
    })

water_df = pd.DataFrame(water_rows).sort_values("date")

air_df.to_csv("data/air_quality.csv", index=False)
fire_df.to_csv("data/wildfires.csv", index=False)
water_df.to_csv("data/water_quality.csv", index=False)

print(f"Generated {len(air_df)} air quality records")
print(f"Generated {len(fire_df)} wildfire records")
print(f"Generated {len(water_df)} water quality records")
print("Saved to data/")
