
import numpy as np
import pandas as pd
import seaborn
import matplotlib.pyplot as plt
import networkx as nx


df = pd.read_parquet("/tmp/daily_bars.parquet")

aggregations = {
    'volume':'min'
}
min_df = df.loc["2022"].groupby("symbol").agg(aggregations)
min_df = min_df[min_df["volume"] > 9000000]
min_df = min_df.reset_index()
symbols = min_df["symbol"].tolist()

pivot_df = df.loc["2022"]
pivot_df = pivot_df[pivot_df["symbol"].isin(symbols)]
pivot_df = pivot_df[["symbol", "open"]]
pivot_df = pivot_df.reset_index()
pivot_df["timestamp"] = pivot_df["timestamp"].dt.strftime("%Y-%m-%d")
pivot_df = pivot_df.pivot("timestamp", "symbol", "open")

corr_df = pivot_df.corr()

tri_df = corr_df.where(~np.tril(np.ones(corr_df.shape)).astype(bool))
tri_df = tri_df.stack()
tri_df = tri_df[tri_df > 0.9]
pairs = tri_df.index.values

G = nx.Graph()
for pair in pairs:
    G.add_edge(*pair)
    
C = list(nx.connected_components(G))

for symbols in C:
    sub_df = df[df["symbol"].isin(symbols)]["open"]
    sub_df.plot()

