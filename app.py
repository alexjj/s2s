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

# Main script to load summits and calculate pairwise arc lengths
if __name__ == "__main__":
    # Load summits from the CSV file
    file_path = 'allsummits.csv'
    summits = pd.read_csv(file_path)

    # Ensure the file has the required columns
    required_columns = {'Latitude', 'Longitude', 'AltM', 'SummitCode'}
    if not required_columns.issubset(summits.columns):
        raise ValueError(f"The file must contain the following columns: {required_columns}")

    # Output file path
    output_path = 'summit_distances.csv'

    # Open the output file for writing
    with open(output_path, mode='w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Write the header
        csvwriter.writerow(['Summit 1', 'Summit 2', 'Arc Length (m)'])

        # Calculate pairwise arc lengths and write them to the file
        for (i, summit1), (j, summit2) in combinations(summits.iterrows(), 2):
            lat1, lon1, alt1, name1 = summit1[1]['Latitude'], summit1[1]['Longitude'], summit1[1]['AltM'], summit1[1]['SummitCode']
            lat2, lon2, alt2, name2 = summit2[1]['Latitude'], summit2[1]['Longitude'], summit2[1]['AltM'], summit2[1]['SummitCode']
            arc_length = calculate_arc_length(lat1, lon1, alt1, lat2, lon2, alt2)
            csvwriter.writerow([name1, name2, arc_length])

    print(f"Pairwise arc lengths have been saved to {output_path}")
