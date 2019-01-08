import os
from os import listdir
import pandas as pd

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

cwd = os.getcwd()
all_csv = find_csv_filenames(cwd)
converted_csv = []
#filter converted files from all_csv
for csv in all_csv:
	if 'converted_' in csv:
		converted_csv.append(csv)
if len(converted_csv) != 0:
	df = pd.concat((pd.read_csv(f, header = 0, index_col=0) for f in converted_csv))
	df.to_csv("convo_clean_input.csv")
	print('[convo_clean_input.csv]')
else:
	print('Please convert your files first!')