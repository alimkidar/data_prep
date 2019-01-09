import os
from os import listdir
import pandas as pd

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

cwd = os.getcwd()
all_csv = find_csv_filenames(cwd)


for csv in all_csv:
	output_name = "converted_" + csv
	columns = {} #columns['lama'] = 'baru'
	df_convo = pd.read_csv(csv, index_col=0)
	headers = list(df_convo)
	follower_status = False
	for x in headers:
		header = x.lower()
		#post_id
		if 'post' in header and 'id' in header:
			columns[header] = 'post_id'
		#caption
		elif 'convo' in header or 'conversation' in header or 'text' in header or 'caption' in header:
			columns[header] = 'caption'
		#comment_count
		elif 'comment' in header:
			columns[header] = 'comment_count'
		#date_post
		elif 'date' in header:
			columns[header] = 'date_post'
		#follower_count
		elif 'follower' in header:
			columns[header] = 'follower_count'
			follower_status = True
		#following_count
		elif 'following' in header:
			columns[header] = 'following_count'
		#like_count
		elif 'like' in header and 'user' not in header:
			columns[header] = 'like_count'
		#timestamp
		elif 'timestamp' in header:
			columns[header] = 'timestamp'
		#user_id
		elif 'user' in header and 'id' in header:
			columns[header] = 'user_id'
		#username
		elif 'user' in header and 'name' in header:
			columns[header] = 'username'
		else:
			columns[header] = header
	new_df_convo = df_convo.rename(index=str, columns=columns)
	list_column = []
	for i in columns.values():
		list_column.append(i)
	if follower_status == False:
		print("[" + csv + " " + 'has no follower/following information]')
		output_name = 'Invalid_' + csv
	new_df_convo[list_column].to_csv(output_name)
print('Done')