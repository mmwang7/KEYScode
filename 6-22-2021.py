# Importing
import pandas as pd
df = pd.read_csv('2016/PMC4207242-arizona-out.tsv', encoding='utf-8', sep='\t')
df

# Dropping unneeded columns
cols = list(df.columns.values)
df = df.drop(columns = cols[5:15])
df

# Remove 'bad' data (uaz code)
pattern = '::uaz:'
df['UAZ?'] = df['OUTPUT'].str.contains(pattern)
df = df[df['UAZ?'] != True]
df = df.drop(columns = 'UAZ?')
df
