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

num_rows = fish_df.shape[0]

def process_row(i):
    edges = []
    current_row = fish_df.iloc[i]
    current_species = current_row[species_cols]
    if current_species.sum() == 0:
        return edges
    for j in range(i + 1, num_rows):
        candidate_row = fish_df.iloc[j]
        candidate_species = candidate_row[species_cols]
        if candidate_species.sum() == 0:
            continue
        shared = (current_species & candidate_species).sum()
        if shared > 0:
            node_name = f"{current_row['Longitude']}_{current_row['Latitude']}"
            candidate_node_name = f"{candidate_row['Longitude']}_{candidate_row['Latitude']}"
            edges.append((node_name, candidate_node_name, shared))
    return edges

print(f"Number of rows in the dataset: {num_rows}")
# Use a ThreadPoolExecutor to parallelize the edge creation
# You CANNOT use ProcessPoolExecutor here because each callable is accessing the same DataFrame
print("Processing rows to create edges...")
edges = []
with concurrent.futures.ThreadPoolExecutor() as executor:
    for result in tqdm(executor.map(process_row, range(num_rows)), total=num_rows):
        edges.extend(result)

print("Edges processed successfully.")
print(f"Total edges created: {len(edges)}")
# Add edges to the graph
for edge in edges:
    G.add_edge(edge[0], edge[1], weight=edge[2])

print("Graph created successfully with edges, saving to file...")
# Save the graph to a GML file
nx.write_gml(G, 'fish_network.gml.gz')
print("Graph saved to 'fish_network.gml.gz'.")
