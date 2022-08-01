
import numpy as np
import pandas as pd
import seaborn
import matplotlib.pyplot as plt


df = pd.read_parquet("/tmp/daily_bars.parquet")

aggregations = {
    'volume':'min'
}
min_df = df.loc["2022"].groupby("symbol").agg(aggregations)
min_df = min_df[min_df["volume"] > 9000000]
min_df = min_df.reset_index()
symbols = min_df["symbol"].tolist()

pivot_df = df[df["symbol"].isin(symbols)]
pivot_df = pivot_df[["symbol", "open"]]
pivot_df = pivot_df.reset_index()
pivot_df["timestamp"] = pivot_df["timestamp"].dt.strftime("%Y-%m-%d")
pivot_df = pivot_df.pivot("timestamp", "symbol", "open")

corr_df = pivot_df.corr()

mask = np.zeros_like(corr_df)
mask[np.triu_indices_from(mask)] = True
seaborn.heatmap(corr_df, cmap='RdYlGn', vmax=1.0, vmin=-1.0 , mask = mask)
plt.yticks(rotation=0) 
plt.xticks(rotation=90) 
plt.show()


