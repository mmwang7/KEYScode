import pandas as pd
import os
import time

# Dropping unneeded columns and data with code 'UAZ'
def clean (file):
    cols = list(file.columns.values)
    file = file.drop(columns = cols[5:15])
    file = file[~file['OUTPUT'].str.contains('::uaz:')]
    file = file[~file['CONTROLLER'].str.contains('::uaz:')]
    return file

# Only applied to rows with EVENT ID as INPUT value
# Change input values and adjust EVENT LABEL of rows
def change_inputs (file):
    pattern = r'E[0-9]'
    counter = 0
    output = ''
    for row in file['INPUT'].str.contains(pattern):
        if (row):
            output = file.iloc[counter, 1]
            file.iloc[counter, 0] = output[:-2]
            if ((output[-1] == 't') | (output[-1] == 'a')):
                file.iloc[counter, 4] = file.iloc[counter, 4].replace('Regulation', 'Activation')
            elif ((output[-1] == 'u') | (output[-1] == 'd')):
                if (file.iloc[counter, 4] == 'Regulation (Positive)'):
                    file.iloc[counter, 4] = file.iloc[counter, 4].replace('Positive', 'Negative')
                else:
                    file.iloc[counter, 4] = file.iloc[counter, 4].replace('Negative', 'Positive')
            else:
                file.iloc[counter, 4] = 'UNKNOWN'
        counter += 1
    return file

# Small file testing
df = pd.read_csv('./2016/PMC4207242-arizona-out.tsv', encoding='utf-8', sep='\t')
df = clean(df)
df = change_inputs(df)
df[25:35]

#Combine files into one big file

paper_path = './2016/'
directory = os.fsencode(paper_path)
big_df = pd.DataFrame()
group = []

for csv in os.listdir(directory):
    csvname = os.fsdecode(csv)
    next_df = pd.read_csv('./2016/' + csvname, encoding='utf-8', sep='\t', engine='python', error_bad_lines=False)
    next_df = clean(next_df)
    group.append(next_df)
    
    if (len(group) == 10):
        big_df = big_df.append(group)
        group = []
        # break
if (len(group) > 0):
    big_df = big_df.append(group)
big_df

# Checking dupicate rows based on INPUT, OUTPUT, and CONTROLLER column
dups_df = big_df[big_df.duplicated(['OUTPUT', 'CONTROLLER']. keep=False)]
dups_df
