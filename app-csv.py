import pandas as pd
import numpy as np
from itertools import combinations, islice
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import csv

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

# Function to compute distances for a chunk of summit pairs and write results to file
def compute_distances_and_write(pair_chunk):
    with open('optimized_distances_chunked.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        results = []
        for i, j in pair_chunk:
            dist = haversine_np(latitudes[i], longitudes[i], latitudes[j], longitudes[j])
            results.append((summit_codes[i], summit_codes[j], round(dist, 2)))
        writer.writerows(results)

# Define chunk size (adjust based on memory availability)
chunk_size = 50000  # Process 50,000 summit pairs at a time

# Use multiprocessing for faster computation
num_workers = max(cpu_count() - 1, 1)  # Leave 1 CPU core free

def main():
    # Initialize CSV file with headers
    with open('optimized_distances_chunked.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Summit1', 'Summit2', 'Distance_km'])

    # Create a multiprocessing pool to process chunks and write results to CSV
    with Pool(num_workers) as pool:
        list(tqdm(
            pool.imap_unordered(compute_distances_and_write, chunked_combinations(range(len(df)), chunk_size)),
            total=(len(df) * (len(df) - 1) // 2) // chunk_size,
            desc="Calculating summit distances"
        ))

    print("Optimized distance calculations saved to optimized_distances_chunked.csv")

if __name__ == "__main__":
    main()
