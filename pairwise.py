#%%
import pandas as pd
from itertools import combinations, islice
import csv

# Function to generate summit combinations in chunks to prevent memory overflow
def chunked_combinations(iterable, chunk_size):
    iterator = iter(combinations(iterable, 2))
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk

# Load your CSV file in chunks
chunk_size = 10000  # Adjust based on your memory availability
df_iter = pd.read_csv('allsummits.csv', chunksize=chunk_size)

# Open the output file for writing
with open('pairwise_summits.csv', mode='w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['summit1', 'lat1', 'lon1', 'alt1', 'summit2', 'lat2', 'lon2', 'alt2'])

    for df in df_iter:
        summit_codes = df['SummitCode'].tolist()
        latitudes = df['Latitude'].tolist()
        longitudes = df['Longitude'].tolist()
        altitudes = df['AltM'].tolist()

        for pair_chunk in chunked_combinations(range(len(summit_codes)), chunk_size):
            results = []
            for i, j in pair_chunk:
                results.append([
                    summit_codes[i], latitudes[i], longitudes[i], altitudes[i],
                    summit_codes[j], latitudes[j], longitudes[j], altitudes[j]
                ])
            csvwriter.writerows(results)

print("Pairwise combinations have been saved to pairwise_summits.csv")







#%%
# Generate pairwise combinations of the SummitCode column
pairwise_combinations = list(combinations(df['SummitCode'], 2))

# Create a new dataframe for the pairwise combinations
pairwise_df = pd.DataFrame(pairwise_combinations, columns=['Summit1', 'Summit2'])

# Merge the original dataframe with the pairwise combinations based on SummitCode
pairwise_df = pairwise_df.merge(df[['SummitCode', 'AltM', 'Longitude', 'Latitude']], left_on='Summit1', right_on='SummitCode', how='left')
pairwise_df = pairwise_df.merge(df[['SummitCode', 'AltM', 'Longitude', 'Latitude']], left_on='Summit2', right_on='SummitCode', how='left', suffixes=('_1', '_2'))

# Drop unnecessary columns and rename them to the desired format
pairwise_df = pairwise_df[['Summit1', 'Latitude_1', 'Longitude_1', 'AltM_1', 'Summit2', 'Latitude_2', 'Longitude_2', 'AltM_2']]

# Rename columns
pairwise_df.columns = ['summit1', 'lat1', 'lon1', 'alt1', 'summit2', 'lat2', 'lon2', 'alt2']

# Save the resulting dataframe to a CSV file if needed
pairwise_df.to_csv('pairwise_summits.csv', index=False)

# Display the first few rows
print(pairwise_df.head())
