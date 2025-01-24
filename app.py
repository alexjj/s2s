import pandas as pd
from geopy.distance import geodesic
from itertools import combinations
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load summit data
df = pd.read_csv('summitsfiltered.csv')

# Prepare the necessary columns for processing
summit_data = df[['SummitCode', 'Latitude', 'Longitude']].to_dict('records')

# Function to compute distance for a summit pair
def compute_distance(pair):
    summit1, summit2 = pair
    coord1 = (summit1['Latitude'], summit1['Longitude'])
    coord2 = (summit2['Latitude'], summit2['Longitude'])
    distance = geodesic(coord1, coord2).kilometers
    return summit1['SummitCode'], summit2['SummitCode'], round(distance, 2)

# Generator function to yield summit pairs in chunks
def summit_pairs_generator(data):
    for pair in combinations(data, 2):  # Yield summit pairs one by one
        yield pair

# Process summit pairs in chunks using multiprocessing
def process_in_chunks():
    with Pool(cpu_count()) as pool:
        # Use tqdm to show progress and map the computation function
        results = pool.imap(compute_distance, summit_pairs_generator(summit_data), chunksize=1000)
        with open('summit_distances_optimized_parallel.csv', 'w') as f:
            f.write('Summit1,Summit2,Distance_km\n')  # Write header
            for result in tqdm(results, total=(len(summit_data)*(len(summit_data)-1))//2, desc="Calculating distances"):
                f.write(f"{result[0]},{result[1]},{result[2]}\n")

# Run the optimized processing function
if __name__ == "__main__":
    process_in_chunks()
    print("Optimized parallelized distance data saved to summit_distances_optimized_parallel.csv")
