import pandas as pd
import numpy as np
import sqlite3
from itertools import combinations, islice
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load the summit data
df = pd.read_csv('summitsfiltered.csv')

# Convert latitude and longitude to radians for faster calculations
df['Latitude_rad'] = np.radians(df['Latitude'])
df['Longitude_rad'] = np.radians(df['Longitude'])
df['Altitude_km'] = df['AltM'] / 1000  # Convert altitude to kilometers

# Constants for WGS-84 ellipsoid
a = 6378.1370  # Equatorial radius in km
b = 6356.7523  # Polar radius in km

def geocentric_radius(lat):
    lat_rad = np.radians(lat)
    return np.sqrt(((a**2 * np.cos(lat_rad))**2 + (b**2 * np.sin(lat_rad))**2) /
                   ((a * np.cos(lat_rad))**2 + (b * np.sin(lat_rad))**2))

# Modified Haversine formula to include altitude and Earth's shape
def haversine_with_altitude(lat1, lon1, alt1, lat2, lon2, alt2):
    R1 = geocentric_radius(np.degrees(lat1)) + alt1
    R2 = geocentric_radius(np.degrees(lat2)) + alt2
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return np.sqrt(R1**2 + R2**2 - 2 * R1 * R2 * np.cos(c))

# Convert lat/lon/alt to arrays for efficient computation
latitudes = df['Latitude_rad'].to_numpy()
longitudes = df['Longitude_rad'].to_numpy()
altitudes = df['Altitude_km'].to_numpy()
summit_codes = df['SummitCode'].to_numpy()

# Function to generate summit combinations in chunks to prevent memory overflow
def chunked_combinations(iterable, chunk_size):
    iterator = iter(combinations(iterable, 2))
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk

# Function to compute distances and write to SQLite (with per-process connection)
def compute_distances_and_write(pair_chunk):
    conn = sqlite3.connect('summits_distances.db', timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")  # Enable WAL mode for better concurrency
    cursor = conn.cursor()

    data = []
    for i, j in pair_chunk:
        dist = haversine_with_altitude(latitudes[i], longitudes[i], altitudes[i],
                                       latitudes[j], longitudes[j], altitudes[j])
        data.append((summit_codes[i], summit_codes[j], round(dist, 2)))

    cursor.executemany("INSERT INTO distances (Summit1, Summit2, Distance_km) VALUES (?, ?, ?)", data)
    conn.commit()
    conn.close()

# Define chunk size (adjust based on memory availability)
chunk_size = 50000  # Process 50,000 summit pairs at a time

# Use multiprocessing for faster computation
num_workers = max(cpu_count() - 1, 1)  # Leave 1 CPU core free

def main():
    # Initialize SQLite database and create table
    conn = sqlite3.connect('summits_distances.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS distances (
            Summit1 TEXT,
            Summit2 TEXT,
            Distance_km REAL
        )
    ''')
    conn.commit()
    conn.close()

    # Create a multiprocessing pool to process chunks and write results to the database
    with Pool(num_workers) as pool:
        list(tqdm(
            pool.imap_unordered(compute_distances_and_write, chunked_combinations(range(len(df)), chunk_size)),
            total=(len(df) * (len(df) - 1) // 2) // chunk_size,
            desc="Calculating summit distances"
        ))

    print("Optimized distance calculations saved to summits_distances.db")

    # Sort the data in the database and print top 10 results
    conn = sqlite3.connect('summits_distances.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM distances ORDER BY Distance_km DESC LIMIT 10")
    top_10 = cursor.fetchall()

    print("\nTop 10 longest summit distances:")
    for row in top_10:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()
