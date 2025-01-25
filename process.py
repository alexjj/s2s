import pandas as pd
import sqlite3

# Load data into SQLite database
conn = sqlite3.connect('large_data.db')
df = pd.read_csv('optimized_distances_chunked.csv')
df.to_sql('summits', conn, if_exists='replace', index=False)

# Query sorted data in SQL
query = "SELECT * FROM summits ORDER BY Distance_km DESC"
df_sorted = pd.read_sql_query(query, conn)

df_sorted.to_csv('sorted_large_file_sql.csv', index=False)
conn.close()

print("Large file sorted using SQLite.")
