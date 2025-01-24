import pandas as pd
import numpy as np
from itertools import combinations
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load the summit data
df = pd.read_csv('summitsfiltered.csv')

# Convert latitude and longitude to radians (for faster calculations)
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

# Generate summit index combinations in chunks to avoid memory issues
summit_pairs = list(combinations(range(len(df)), 2))

# Function to compute distances for a subset of summit pairs
def compute_distances(pair_chunk):
    results = []
    for i, j in pair_chunk:
        dist = haversine_np(latitudes[i], longitudes[i], latitudes[j], longitudes[j])
        results.append((summit_codes[i], summit_codes[j], round(dist, 2)))
    return results

# Split summit pairs into manageable chunks based on CPU count
num_workers = cpu_count()
chunk_size = len(summit_pairs) // num_workers

# Use multiprocessing to speed up computations
with Pool(num_workers) as pool:
    results = list(
        tqdm(
            pool.imap(compute_distances, [summit_pairs[i:i + chunk_size] for i in range(0, len(summit_pairs), chunk_size)]),
            total=num_workers,
            desc="Calculating summit distances"
        )
    )

# Flatten results from parallel processing
flat_results = [item for sublist in results for item in sublist]

# Convert results to a DataFrame
distance_df = pd.DataFrame(flat_results, columns=['Summit1', 'Summit2', 'Distance_km'])

# Sort distances in descending order and save to CSV
distance_df.sort_values(by='Distance_km', ascending=False).to_csv('optimized_distances.csv', index=False)

print("Optimized distance calculations saved to optimized_distances.csv")
