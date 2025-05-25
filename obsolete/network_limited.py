import pandas as pd
import networkx as nx
from tqdm.notebook import tqdm
import concurrent.futures

print("Loading data...")
fish_df = pd.read_csv('fish.csv', sep=';')
print("Data loaded successfully.")

G = nx.Graph()

# Get the list of species columns (all columns except the first, Longitude, and Latitude)
species_cols = fish_df.columns.difference(['Unnamed: 0', 'Longitude', 'Latitude'])

fish_df = fish_df[
    (fish_df['Longitude'] > 131) &
    (fish_df['Longitude'] < 194) &
    (fish_df['Latitude'] < 48) &
    (fish_df['Latitude'] > 17)
].reset_index(drop=True)

num_rows = fish_df.shape[0]
print(f"Filtered dataset contains {num_rows} rows.")

for i in range(num_rows):
    current_row = fish_df.iloc[i]
    current_species = current_row[species_cols]
    # log every 500 rows processed
    if i % 500 == 0:
        print(f"Processing row {i} of {num_rows}")
    if current_species.sum() == 0:
        continue  # skip rows with all zeroes in species columns
    # Iterate through the remaining rows to find connections
    # with shared species
    # This avoids checking pairs that have already been checked (the graph is undirected)
    for j in range(i + 1, num_rows):
        if j % 500 == 0:
            print(f"Processing pair row id {j} (only {num_rows - i - 1} rows left to check)")
        # Get the candidate row
        candidate_row = fish_df.iloc[j]
        candidate_species = candidate_row[species_cols]
        if candidate_species.sum() == 0:
            continue

        intersection = current_species & candidate_species
        intersection_sum = intersection.sum()
        # Find columns where both rows have a 1 (i.e., both have the species)
        if intersection_sum > 0:
            # node name is concatenation of the longitude and latitude
            node_name = f"{current_row['Longitude']}_{current_row['Latitude']}"
            # second node name is constructed similarly
            candidate_node_name = f"{candidate_row['Longitude']}_{candidate_row['Latitude']}"
            # Add an edge between the two nodes
            G.add_edge(node_name, candidate_node_name, weight=intersection_sum)

print("Graph created successfully with edges, saving to file...")
# Save the graph to a GML file
nx.write_gml(G, 'fish_network.gml.gz')
print("Graph saved to 'fish_network.gml.gz'.")

import matplotlib.pyplot as plt

plt.figure(figsize=(12, 12))
pos = nx.spring_layout(G, k=0.1, iterations=20)
nx.draw_networkx_nodes(G, pos, node_size=10)
nx.draw_networkx_edges(G, pos, alpha=0.2)
plt.title("Fish Coordinates Graph")
plt.axis('off')
plt.show()

