import math
import pandas as pd
from itertools import combinations
import csv

def calculate_arc_length(lat1, lon1, alt1, lat2, lon2, alt2):
    """
    Calculate the circumference segment (arc length) between two mountain summits.

    Parameters:
        lat1, lon1: Latitude and Longitude of the first summit in degrees.
        alt1: Altitude of the first summit in meters.
        lat2, lon2: Latitude and Longitude of the second summit in degrees.
        alt2: Altitude of the second summit in meters.

    Returns:
        Arc length in meters.
    """
    # Earth's equatorial and polar radii in meters
    a = 6378137.0  # in meters
    b = 6356752.3  # in meters

    # Convert latitudes and longitudes from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Calculate the radius of the Earth at a given latitude using the formula for an ellipsoid
    def earth_radius_at_latitude(lat):
        cos_lat = math.cos(lat)
        sin_lat = math.sin(lat)
        numerator = ((a**2) * (cos_lat)**2 + (b**2) * (sin_lat)**2)
        denominator = (cos_lat)**2 + ((b / a)**2) * (sin_lat)**2
        return math.sqrt(numerator / denominator)

    # Calculate the radii at the two latitudes
    radius1 = earth_radius_at_latitude(lat1_rad) + alt1
    radius2 = earth_radius_at_latitude(lat2_rad) + alt2

    # Calculate the central angle
    delta_lon = lon2_rad - lon1_rad
    central_angle = math.acos(math.sin(lat1_rad) * math.sin(lat2_rad) + math.cos(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon))

    # Calculate the arc length
    arc_length = ((radius1 + radius2) / 2) * central_angle

    return arc_length

# Main script to load pairwise summits and calculate arc lengths
if __name__ == "__main__":
    input_file = 'pairwise_summits.csv'
    output_file = 'summit_distances.csv'
    chunk_size = 10000

    # Open the output file for writing
    with open(output_file, mode='w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Summit 1', 'Summit 2', 'Arc Length (m)'])

        # Load the pairwise summits in chunks
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            results = []
            for _, row in chunk.iterrows():
                lat1, lon1, alt1 = row['lat1'], row['lon1'], row['alt1']
                lat2, lon2, alt2 = row['lat2'], row['lon2'], row['alt2']
                arc_length = calculate_arc_length(lat1, lon1, alt1, lat2, lon2, alt2)
                results.append([row['summit1'], row['summit2'], arc_length])
            csvwriter.writerows(results)

    print(f"Arc lengths have been saved to {output_file}")
