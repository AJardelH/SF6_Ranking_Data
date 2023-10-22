import glob
import pandas as pd
import os

# Get CSV files list from cwd
path = os.getcwd()
csv_files = glob.glob(path + "/*.csv")

# Read each CSV file into DataFrame
# This creates a list of dataframes
df_list = (pd.read_csv(file) for file in csv_files)

# Concatenate all DataFrames
big_df   = pd.concat(df_list, ignore_index=True)

#export big csv
pd.DataFrame.to_csv(big_df,'all_ranks.csv')
