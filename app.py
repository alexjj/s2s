import pandas as pd
import numpy as np
from itertools import combinations, islice
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load the summit data
df = pd.read_csv('summitsfiltered.csv')

# Convert latitude and longitude to radians for faster calculations
df['Latitude_rad'] = np.radians(df['Latitude'])
df['Longitude_rad'] = np.radians(df['Longitude'])

# Haversine formula using NumPy
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

# Function to compute distances for a chunk of summit pairs
def compute_distances(pair_chunk):
    results = []
    for i, j in pair_chunk:
        dist = haversine_np(latitudes[i], longitudes[i], latitudes[j], longitudes[j])
        results.append((summit_codes[i], summit_codes[j], round(dist, 2)))
    return results

# Define chunk size (adjust based on memory availability)
chunk_size = 100000  # Process 100,000 summit pairs at a time

# Use multiprocessing for faster computation
num_workers = cpu_count()

# Create a multiprocessing pool to process chunks
with Pool(num_workers) as pool:
    results = []
    for chunk_result in tqdm(pool.imap(compute_distances, chunked_combinations(range(len(df)), chunk_size)),
                             total=(len(df) * (len(df) - 1) // 2) // chunk_size,
                             desc="Calculating summit distances"):
        results.extend(chunk_result)

# Convert results to DataFrame
distance_df = pd.DataFrame(results, columns=['Summit1', 'Summit2', 'Distance_km'])

# Sort distances in descending order and save to CSV
distance_df.sort_values(by='Distance_km', ascending=False).to_csv('optimized_distances_chunked.csv', index=False)

print("Optimized distance calculations saved to optimized_distances_chunked.csv")
