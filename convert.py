import os, re, datetime
from os import listdir
import pandas as pd
from logger import Logger
time_now = str(int(datetime.datetime.now().timestamp()))

#find duplicate value in a list
def find_duplicate_in_list(list_):
	list_set = set(list_)
	for i in list_set:
		list_.remove(i)
	return list_

#delete fieldname which name is contain 'Unnamed'
def anti_unnamed(df):
	unnamed_bucket = []
	fieldname_list = list(df)
	for i in fieldname_list:
		if 'unnamed' in i.lower():
			unnamed_bucket.append(i)
	df.drop(unnamed_bucket,axis=1, inplace=True)
	return df
#collect all csv file in particular folder
def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith(suffix) ]
#remove all spacebar in text
def no_space(text_):
	space_satus = True
	text_ = str(text_) + ' '
	while space_satus == True:
		if ' ' in text_:
			text_ = text_.replace(' ','')
		else:
			space_satus = False
		text_ = text_.lower()
	return text_

#converting string into a list by particular charachter. Tokenization.
def split_by(word_, char_):
	word = word_.split(char_)
	return(word)
#concat duplicate columns/fieldnames into a single columm/fieldname
def concat_duplicate_columns(df):
    dupli = {}
    # populate dictionary with column names and count for duplicates 
    for column in df.columns:
        dupli[column] = dupli[column] + 1 if column in dupli.keys() else 1
    # rename duplicated keys with °°° number suffix
    for key, val in dict(dupli).items():
        del dupli[key]
        if val > 1:
            for i in range(val):
                dupli[key+'°°°'+str(i)] = val
        else: dupli[key] = 1
    # rename columns so that we can now access abmigous column names
    # sorting in dict is the same as in original table
    df.columns = dupli.keys()
    # for each duplicated column name
    for i in set(re.sub('°°°(.*)','',j) for j in dupli.keys() if '°°°' in j):
        i = str(i)
        # for each duplicate of a column name
        for k in range(dupli[i+'°°°0']-1):
            # concatenate values in duplicated columns
            df[i+'°°°0'] = df[i+'°°°0'].astype(str) + df[i+'°°°'+str(k+1)].astype(str)
            # Drop duplicated columns from which we have aquired data
            df = df.drop(i+'°°°'+str(k+1), 1)
    # resort column names for proper mapping
    df = df.reindex_axis(sorted(df.columns), axis = 1)
    # rename columns
    df.columns = sorted(set(re.sub('°°°(.*)','',i) for i in dupli.keys()))
    return df

#check duplicate column/fieldname in dataframe
def check_duplicated_in_df(df):
	fieldname = list(df)
	duplicate = find_duplicate_in_list(fieldname)
	return duplicate
#look up csv file that contains duplicate columns/fieldnames
def duplicate_check_in_df(csv, df):
	duplicate = check_duplicated_in_df(df)
	if len(duplicate) > 0:
		error_duplicate(csv, df)
#get error and notify the user about duplicate column/fieldname
def error_duplicate(csv, df):
	duplicated_field = find_duplicate_in_list(list(df))
	print(' ')
	print ("Error: in file", csv, 'there are 2 or more similar columns!', str(duplicated_field))
	quit()


#Load Rules
try:
	df_rules = pd.read_csv('config/rules.csv')
except:
	print("Error: can't find rules.csv")
	quit()

cwd = os.getcwd()
all_csv = find_csv_filenames(cwd + r'/input/')


fieldname_suffix_ = list(df_rules['fieldname'])
fieldname_suffix = sorted(set(fieldname_suffix_), key=fieldname_suffix_.index) 

df_all = pd.DataFrame()
df_temp = pd.DataFrame()

'''if 'rules.csv' in all_csv:
	all_csv.remove('rules.csv')
else:
	print("Error: can't find rules.csv")
	quit()'''

output_name = cwd + r"/output/convo.csv"
log_name = cwd + r"/temp/convo_converted_" + time_now + ".csv"
list_field_rule = []
for csv in all_csv:
	columns = {} #columns['old'] = 'new'
	df_convo = pd.read_csv(cwd + r'/input/' + csv, index_col=0, encoding='utf-8',low_memory=False, dtype=str)
	duplicate_check_in_df(csv, df_convo)
	headers = list(df_convo)
	follower_status = False	
	print('			[Converting ' + csv + ']')
	# for every fieldname in input file
	for field_input_ in headers:
		field_input = field_input_.lower()

		#for every rule from rules.csv
		for index, row in df_rules.iterrows():
			fieldname_rule = row['fieldname']
			contain = split_by(no_space(row['contain']), '+')
			not_contain = split_by(no_space(row['not_contain']), '+')
		
			#listing fieldname rule into a dict
			if fieldname_rule not in list_field_rule:
				list_field_rule.append(fieldname_rule)

			len_contain = len(contain)
			len_not_contain = len(not_contain)
			#matching fieldname input with fieldname output
			for word in contain:
				if str(word) in str(field_input):
					len_contain -= 1
			for word in not_contain:
				if str(word) not in str(field_input):
					len_not_contain -= 1
			#print(field_input_, '==>',fieldname_rule, 'len_contain', str(len_contain),'len_not_contain', str(len_not_contain))

			if len_contain < 1 and len_not_contain < 1:
				columns[field_input_] = fieldname_rule
				print(field_input + ' => ' + fieldname_rule)
				break
			else:
				columns[field_input_] = field_input_
	print('			[File ' + csv + ' was successfully added.]')
	new_df_convo = df_convo.rename(columns=columns)
	duplicate_check_in_df(csv, new_df_convo)
	try: #merge files
		df_temp = pd.concat([new_df_convo, df_all], sort=False)
	except AssertionError:
		error_duplicate(csv, new_df_convo)
	except ValueError:
		error_duplicate(csv, df_all)
	df_all = anti_unnamed(df_temp)

#dict for listing the column
list_column = []
for i in columns.values():
	list_column.append(i)
#Add fieldname if there is no fieldname_rule in input_files
for i in list_field_rule:
	if i not in list_column:
		df_all[i] = ''


# customizing post_id, user_id, and caption
df_all['post_id'] = df_all['post_id'].apply(lambda x: str(x).replace('"','').replace("'",""))
df_all['user_id'] = df_all['user_id'].apply(lambda x: str(x).replace('"','').replace("'",""))
df_all['caption'] = df_all['caption'].apply(lambda x: str(x).replace('b"','').replace("b'","").replace('\n',' ').replace('\t',' ').replace('\r',' '))
df_all['caption'] = df_all['caption'].apply(lambda x: str(x).replace('"','').replace("'",""))
df_all['post_id'] = "'" + df_all['post_id']
df_all['user_id'] = "'" + df_all['user_id']
df_all['caption'] = "'" + df_all['caption'] 

#Delete Index
fieldname_df_all = list(df_all)
if 'index' in fieldname_df_all:
	df_all = df_all.drop(columns=['index'])
df_all.reset_index()
df_all.index.names = ['index']
fieldname_to_remove = ['post_id','user_id','caption']


final_list = list(set(fieldname_df_all).difference(set(fieldname_suffix)))
fieldname_output = fieldname_suffix + final_list
df_all = df_all[fieldname_output]

#deduplicate by caption, username, timestamp
df_all = df_all.drop_duplicates(subset=['caption', 'username', 'timestamp'], keep='first')
df_all.to_csv(output_name, index=False)
df_all.to_csv(log_name, index=False)
file_input = str(all_csv).replace('[','{').replace(']','}')
log_text = '[' + time_now + ' add ' + file_input + ' into '+ log_name + ']\n'
Logger(log_text).write_log()
print('		[saved as ' + output_name+']')