import csv
from geopy.distance import geodesic

def get_coordinates(summit_code, csv_file='summitslist.csv'):
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['SummitCode'] == summit_code:
                return float(row['Latitude']), float(row['Longitude'])
    raise ValueError(f"Summit code {summit_code} not found in {csv_file}")

def calculate_distance(summit1, summit2):
    coord1 = get_coordinates(summit1)
    coord2 = get_coordinates(summit2)
    distance = geodesic(coord1, coord2).kilometers
    return distance

if __name__ == "__main__":
    summit_pairs = [
        ("EA1/OU-001", "ZL3/MB-161"),
        ("EA1/LE-267", "ZL3/CB-293"),
        ("CT/MN-012", "ZL3/WC-573"),
        ("EA1/SG-012", "ZL1/WL-097"),
        ("EA1/LE-153", "ZL3/CB-506"),
        ("EA1/OU-032", "ZL3/MB-087"),
        ("CT/TM-015", "ZL3/TM-226"),
        ("CT/TM-039", "ZL3/MB-084"),
        ("EA4/TO-040", "ZL1/HB-061"),
        ("EA1/LU-050", "ZL3/CB-719")
    ]

    for summit1, summit2 in summit_pairs:
        try:
            distance = calculate_distance(summit1, summit2)
            print(f"The distance between {summit1} and {summit2} is {distance:.2f} km")
        except ValueError as e:
            print(e)
