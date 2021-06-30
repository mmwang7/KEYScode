# Importing

import pandas as pd
import os
df = pd.read_csv('2016/PMC4207242-arizona-out.tsv', encoding='utf-8', sep='\t')

# Dropping unneeded columns

cols = list(df.columns.values)
df = df.drop(columns = cols[5:15])
df.head()

# Remove "uaz" code data

df['UAZ?'] = df['OUTPUT'].str.contains('::uaz:')
df = df[df['UAZ?'] != True]
df = df.drop(columns = 'UAZ?')
df.head()

# Replacing inputs with EVENT IDs
# Change EVENT LABEL column

pattern = r'E[0-9]'

counter = 0
output = ""
for row in df['INPUT'].str.contains(pattern):
    if (row):
        output = df.iloc[counter, 1]
        df.iloc[counter, 0] = output[:-2]
        
        df.iloc[counter, 4].replace("Regulation", "Activation")
        if ((output[-1] == 'u') & (df.iloc[counter, 4] == 'Regulation (Positive)')):
            df.iloc[counter, 4].replace("Positive", "Negative")
        else:
            df.iloc[counter, 4].replace("Negative", "Positive")
    counter += 1
df[25:35]

#Combine files into one big file

for csv in os.listdir('2016')