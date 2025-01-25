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

# Haversine formula using NumPy for great-circle distance calculation
def haversine_np(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of Earth in km
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

# Convert lat/lon to arrays for efficient computation
latitudes = df['Latitude_rad'].to_numpy()
longitudes = df['Longitude_rad'].to_numpy()
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
        dist = haversine_np(latitudes[i], longitudes[i], latitudes[j], longitudes[j])
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
