import psycopg2


conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="environment_backend",
    user="admin",
    password="admin"
)

cur = conn.cursor()

countries = [
    ("US", "United States"),
    ("GB", "United Kingdom"),
    ("DE", "Germany"),
    ("JP", "Japan"),
    ("KR", "South Korea"),
    ("SG", "Singapore"),
]

cur.executemany("""
    INSERT INTO countries (country_code, country_name)
    VALUES (%s, %s)
    ON CONFLICT (country_code) DO NOTHING;
""", countries)

providers = [
    ("Open-Meteo", "API", "https://open-meteo.com", "Hourly", "Weather and air quality API provider"),
]

cur.executemany("""
    INSERT INTO data_providers
    (provider_name, provider_type, base_url, update_frequency, description)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (provider_name) DO NOTHING;
""", providers)

pollutants = [
    ("PM2.5", "Fine particulate matter", "µg/m³", "Particles with diameter 2.5 micrometers or smaller"),
    ("PM10", "Coarse particulate matter", "µg/m³", "Particles with diameter 10 micrometers or smaller"),
    ("CO", "Carbon monoxide", "µg/m³", "Carbon monoxide concentration"),
    ("NO2", "Nitrogen dioxide", "µg/m³", "Nitrogen dioxide concentration"),
    ("SO2", "Sulfur dioxide", "µg/m³", "Sulfur dioxide concentration"),
    ("O3", "Ozone", "µg/m³", "Ground-level ozone concentration"),
]

cur.executemany("""
    INSERT INTO pollutants
    (pollutant_code, pollutant_name, unit, description)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (pollutant_code) DO NOTHING;
""", pollutants)

cities = [
    ("US", "New York", 40.7128, -74.0060),
    ("US", "Los Angeles", 34.0522, -118.2437),
    ("US", "Chicago", 41.8781, -87.6298),

    ("GB", "London", 51.5074, -0.1278),
    ("GB", "Manchester", 53.4808, -2.2426),

    ("DE", "Berlin", 52.5200, 13.4050),
    ("DE", "Munich", 48.1351, 11.5820),

    ("JP", "Tokyo", 35.6762, 139.6503),
    ("JP", "Osaka", 34.6937, 135.5023),

    ("KR", "Seoul", 37.5665, 126.9780),
    ("KR", "Busan", 35.1796, 129.0756),

    ("SG", "Singapore", 1.3521, 103.8198),
]

for country_code, city_name, lat, lon in cities:
    cur.execute(
        "SELECT country_id FROM countries WHERE country_code = %s",
        (country_code,)
    )
    country_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO cities (country_id, city_name, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (country_id, city_name, lat, lon))

cur.execute("SELECT provider_id FROM data_providers WHERE provider_name = 'Open-Meteo'")
provider_id = cur.fetchone()[0]

cur.execute("SELECT city_id, city_name, latitude, longitude FROM cities")

for city_id, city_name, lat, lon in cur.fetchall():
    cur.execute("""
        INSERT INTO stations
        (city_id, provider_id, station_name, source_station_id, latitude, longitude, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, TRUE);
    """, (
        city_id,
        provider_id,
        f"{city_name} Monitoring Station",
        f"{city_name.lower().replace(' ', '_')}_station",
        lat,
        lon
    ))

aqi_levels = [
    ("PM2.5", "US EPA", 0.0, 12.0, "Good", "Air quality is satisfactory"),
    ("PM2.5", "US EPA", 12.1, 35.4, "Moderate", "Acceptable air quality"),
    ("PM2.5", "US EPA", 35.5, 55.4, "Unhealthy for Sensitive Groups", "Sensitive groups may be affected"),
    ("PM2.5", "US EPA", 55.5, 150.4, "Unhealthy", "Everyone may begin to experience health effects"),
    ("PM2.5", "US EPA", 150.5, 250.4, "Very Unhealthy", "Health alert"),
    ("PM2.5", "US EPA", 250.5, 500.4, "Hazardous", "Health warning of emergency conditions"),
]

for pollutant_code, standard_name, min_value, max_value, level, impact in aqi_levels:
    cur.execute(
        "SELECT pollutant_id FROM pollutants WHERE pollutant_code = %s",
        (pollutant_code,)
    )
    pollutant_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO aqi_standards
        (pollutant_id, standard_name, min_value, max_value, aqi_level, health_impact)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, (pollutant_id, standard_name, min_value, max_value, level, impact))

conn.commit()
cur.close()
conn.close()

print("Seed backend database successfully.")