# Importing and defining fuctions

import pandas as pd
import os
import timeit
import numpy as np

# Dropping unneeded columns
# Remove rows with code 'UAZ' in OUTPUT and CONTROLLER columns
# Remove rows with 'NONE' in CONTROLLER column
def clean (frame):
    frame.drop(frame.columns[[5, 6, 13, 14, 15, 16, 17]], axis=1, inplace=True)
    col_list =  frame.columns.tolist()
    col_list = col_list[0:5] + col_list[-1:] + col_list[5:-1]
    frame = frame.loc[:, col_list]
    
    frame = change_outputs(frame)
    frame.drop(frame.index[frame['OUTPUT'].str.contains('::uaz:')], inplace=True)
    frame.reset_index(drop=True, inplace=True)
    frame = eventID_inputs(frame)
    frame = change_inputs(frame)
    frame.drop(frame.index[frame['CONTROLLER'].str.contains('::uaz:')], inplace=True)
    frame.drop(frame.index[frame['CONTROLLER'].str.contains('NONE')], inplace=True)
    frame.drop(columns='EVENT ID', inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame

def drop_context (frame):
    frame.drop(frame.columns[-6:], axis=1, inplace=True)
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
    col_list =  df.columns.tolist()
    col_list = col_list[0:df.columns.get_loc('EVENT LABEL')] + col_list[-1:] + col_list[df.columns.get_loc('EVENT LABEL'):-1]
    df = df[col_list]
    df = df.drop(columns='EVENT LABEL')
    return df

def get_dbID (df, col_name):
    db_id = col_name + ' DATABASE ID'
    names = col_name + ' NAME'
    df[col_name] = df[col_name].str.strip('\{')
    df[col_name] = df[col_name].str.strip('\}')
    for index, row in df.iterrows():
        if (row[col_name].count('::') > 1):
            row[col_name] = row[col_name].split(', ')
            row[col_name] = [val.split('::') for val in row[col_name]]
            idlist = [item[1] for item in row[col_name]]
            df.at[index, names] = [item[0] for item in row[col_name]]
            df.at[index, db_id] = [id.split('.', 1)[0] for id in [item[1] for item in row[col_name]]]
        else:
            df.at[index, names] = [row[col_name].split('::')[0]]
            df.at[index, db_id] = [row[col_name].split('::')[1].split('.', 1)[0]]
    # Rearraging
    col_list =  df.columns.tolist()
    col_list = col_list[0:df.columns.get_loc(col_name)] + col_list[-2:] + col_list[df.columns.get_loc(col_name):-2]
    df = df[col_list]
    df = df.drop(columns=col_name)
    return df

def drop_dups (df):
    df = df[df.duplicated(subset=['OUTPUT DATABASE ID', 'CONTROLLER DATABASE ID', 'INTERACTION VALUE'], keep=False)]
    df = dups_df.sort_values(by=['OUTPUT DATABASE ID', 'CONTROLLER DATABASE ID', 'INTERACTION VALUE'], ignore_index=True)
    df = dups_df.groupby(by=['OUTPUT DATABASE ID', 'CONTROLLER DATABASE ID', 'INTERACTION VALUE'], as_index=False).size()['size']
    df = dups_df.drop_duplicates(subset=['OUTPUT DATABASE ID', 'CONTROLLER DATABASE ID', 'INTERACTION VALUE'], ignore_index=True)
    df['DUP COUNT'] = dup_count
    return df

# Small file testing
small_df = pd.read_csv('./2016/PMC4486081-arizona-out.tsv', encoding='utf-8', sep='\t')
small_df = clean(small_df)
small_df = drop_context(small_df)
small_df = get_dbID(small_df, 'INPUT')
small_df = get_dbID(small_df, 'OUTPUT')
small_df = get_dbID(small_df, 'CONTROLLER')
small_df.tail(20)

#Combine files into one big file

start = timeit.default_timer()
paper_path = './2016/'
directory = os.fsencode(paper_path)
cols = ['INPUT', 'OUTPUT', 'CONTROLLER', 'EVENT LABEL', 'SEEN IN', 'CONTEXT (SPECIES)', 'CONTEXT (ORGAN)', 
        'CONTEXT (CELL LINE)', 'CONTEXT (CELL TYPE)', 'CONTEXT (CELLULAR COMPONENT)', 'CONTEXT (TISSUE TYPE)']
big_df = pd.DataFrame(columns=cols)
big_df.to_csv('./allfiles.csv', index=False)
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
        big_df = pd.read_csv('./allfiles.csv', encoding='utf-8')
        big_df = big_df.append(group)
        big_df.to_csv('./allfiles.csv', index=False)
        big_df = pd.DataFrame(columns=cols)
        group = []
if (len(group) > 0):
    big_df = pd.read_csv('./allfiles.csv', encoding='utf-8')
    big_df = big_df.append(group)
    big_df.to_csv('./allfiles.csv', index=False)
stop = timeit.default_timer()
print('Time: ', stop - start)

new_df = pd.read_csv('./allfiles.csv', encoding='utf-8')
new_df = get_interVal(new_df)
new_df = get_dbID(new_df, 'INPUT')
new_df = get_dbID(new_df, 'OUTPUT')
new_df = get_dbID(new_df, 'CONTROLLER')
new_df.to_csv('./no_context.csv', index=False)

context_df = pd.read_csv('./context.csv', encoding='utf-8')
nocontext_df = pd.read_csv('./no_context.csv', encoding='utf-8')

# Counting up and dropping dupicate rows based on same OUTPUT, CONTROLLER, and EVENT LABEL column
context_df = drop_dups(context_df)
nocontext_df = drop_dups(nocontext_df)
context_df.to_csv('./context.csv', index=False)
nocontext_df.to_csv('./no_context.csv', index=False)

nocontext_df