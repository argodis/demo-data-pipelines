
import numpy as np
import pandas as pd


pd.set_option('display.max_rows', None)

df = pd.read_parquet("/tmp/datalake/landing/alpaca/bars/historical-daily-bars/minute")

df = df.between_time("9:30", "17:45")

c_df = df.loc["2022-01-06"]
print(c_df.head(1000))
df = df.reset_index()
df["delta"] = df.groupby("symbol")["timestamp"].diff()
df = df.where(df["delta"] > np.timedelta64(1, "m")).dropna()

df["seconds"] = df["delta"].dt.seconds

plot_df = df[["timestamp", "symbol", "seconds"]].copy()

plot_df.set_index(plot_df["timestamp"], inplace = True)
plot_df = plot_df.drop(columns=["timestamp"])

plot_df = plot_df.loc["2022-01-06"]
plot_df = plot_df.where(plot_df["seconds"] < 1000).dropna()
plot_df = plot_df.where(plot_df["symbol"] == "AAPL").dropna()

#plot_df.head()
#plot_df.plot(y="seconds")
