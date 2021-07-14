# Importing and defining fuctions
import pandas as pd
import os
import timeit
import numpy as np

# Dropping unneeded columns
# Remove rows with code 'UAZ' in OUTPUT and CONTROLLER columns
# Remove rows with 'NONE' in CONTROLLER column
def clean (frame):
    frame.drop(frame.columns[5:15], axis=1, inplace=True)
    frame = change_outputs(frame)
    frame.drop(frame.index[frame['OUTPUT'].str.contains('::uaz:')], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame = eventID_inputs(frame)
    frame = change_inputs(frame)
    frame.drop(frame.index[frame['CONTROLLER'].str.contains('::uaz:')], inplace=True)
    frame.drop(frame.index[frame['CONTROLLER'].str.contains('NONE')], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame

# Changing OUTPUT value of rows with EVENT ID
def change_outputs (df):
    event_output = df.index[~df['OUTPUT'].str.contains(':')].tolist()
    for val in event_output:
        base_row = df.index[df['EVENT ID'].str.fullmatch(df.iloc[val, 1])][0]
        df.iloc[val, 1] = df.iloc[base_row, 1]
    return df

# Only applied to rows with EVENT ID as INPUT value
# Change input values and adjust EVENT LABEL of rows
def eventID_inputs (df):
    direct_events = ['Activation', 'Transcription', 'Amount']
    inverse_events = ['Ubiquitination', 'DecreaseAmount']
    colon_lst = df.index[~df['INPUT'].str.contains(':')].tolist()
    for val in colon_lst:
        base_row = df.index[df['EVENT ID'].str.fullmatch(df.iloc[val, 0])][0]
        df.iloc[val, 0] = df.iloc[base_row, 0]
        base_event = df.iloc[base_row, 4]
        if any(event in base_event for event in direct_events):
            df.iloc[val, 4] = df.iloc[val, 4].replace('Regulation', 'Activation',)
        elif any(event in base_event for event in inverse_events):
            df.iloc[val, 4] = df.iloc[val, 4].replace('Regulation (Positive)', 'Activation (Negative)')
            df.iloc[val, 4] = df.iloc[val, 4].replace('Regulation (Negative)', 'Activation (Positive)')
        else:
            df.iloc[val, 4] = 'UNKNOWN'
    return df

# Change rows with EVENT ID as INPUT value
# Cleans up values not properly replaced by eventID_inputs
def change_inputs (df):
    colon_lst = df.index[~df['INPUT'].str.contains(':')].tolist()
    for val in colon_lst:
        base_row = df.index[df['EVENT ID'].str.fullmatch(df.iloc[val, 0])][0]
        df.iloc[val, 0] = df.iloc[base_row, 0]
        base_event = df.iloc[base_row, 4]
    return df

# Creates new column that classifies each row based on EVENT LABEL
# 1 = 'Activation (Positive)'
# -1 = 'Activation (Negative)'
# 0 = Everything else
def get_interVal (df):
    conds = [df['EVENT LABEL'].eq('Activation (Positive)'), df['EVENT LABEL'].eq('Activation (Negative)')]
    choices = [1, -1]
    df['INTERACTION VALUE'] = np.select(conds, choices, default=0)
    return df

def get_dbID (df):
    df['DATABASE ID'] = df['OUTPUT'].str.strip('\{')
    df['DATABASE ID'] = df['DATABASE ID'].str.strip('\}')
    df['DATABASE ID'] = df['DATABASE ID'].str.split(',')
    # df['DATABASE ID'] = df['DATABASE ID'].str.split('::')
    # df['DATABASE ID'] = df['DATABASE ID'].str.split('::')
    return df

# Small file testing
small_df = pd.read_csv('./2016/PMC4207242-arizona-out.tsv', encoding='utf-8', sep='\t')
small_df = clean(small_df)
small_df[25:35]

#Combine files into one big file
start = timeit.default_timer()
paper_path = './2016/'
directory = os.fsencode(paper_path)
cols = ['INPUT', 'OUTPUT', 'CONTROLLER', 'EVENT ID', 'EVENT LABEL', 'TRIGGERS', 'SEEN', 'EVIDENCE', 'SEEN IN']
big_df = pd.DataFrame(columns=cols)
big_df.to_csv('./bigfile.csv', index=False)
group = []
for csv in os.listdir(directory):
    try:
        csvname = os.fsdecode(csv)
        next_df = pd.read_csv('./2016/' + csvname, encoding='utf-8', sep='\t', engine='python', error_bad_lines=False)
        next_df = clean(next_df)
        group.append(next_df)
    except IndexError:
        print(csvname)
    if (len(group) == 10000):
        big_df = pd.read_csv('./bigfile.csv', encoding='utf-8')
        big_df = big_df.append(group)
        big_df.to_csv('./bigfile.csv', index=False)
        big_df = pd.DataFrame(columns=cols)
        group = []
if (len(group) > 0):
    big_df = pd.read_csv('./bigfile.csv', encoding='utf-8')
    big_df = big_df.append(group)
    big_df.to_csv('./bigfile.csv', index=False)
stop = timeit.default_timer()
print('Time: ', stop - start)

total_df = pd.read_csv('./bigfile.csv', encoding='utf-8')

# n = len(pd.unique(total_df['SEEN IN']))
# print(n)
total_df = get_interVal(total_df)
total_df[30:50]

# Counting up and dropping dupicate rows based on same OUTPUT, CONTROLLER, and EVENT LABEL column
dups_df = total_df[total_df.duplicated(['OUTPUT', 'CONTROLLER', 'INTERACTION VALUE'])]
dups_df = dups_df.sort_values(by=['INPUT', 'CONTROLLER'], ignore_index=True)
dups_df.groupby(by=['OUTPUT', 'CONTROLLER', 'INTERACTION VALUE'], as_index=False).size()