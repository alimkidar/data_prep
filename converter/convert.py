import os
from os import listdir
import pandas as pd

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

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
def split_by(word_, char_):
	word = word_.split(char_)
	return(word)
#Load Rules
df_rules = pd.read_csv('rules.csv')
cwd = os.getcwd()
all_csv = find_csv_filenames(cwd)

df_all = pd.DataFrame()
df_temp = pd.DataFrame()

if 'rules.csv' in all_csv:
	all_csv.remove('rules.csv')
else:
	print("Error! There is no rules (rules.csv).")
	print(' ')
	print(' ')
	print(' ')

output_name = "convo_converted.csv"
list_field_rule = []
for csv in all_csv:
	columns = {} #columns['old'] = 'new'
	df_convo = pd.read_csv(csv, index_col=False, encoding='utf-8')
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
				break
			else:
				columns[field_input_] = field_input_
	print('			[File ' + csv + ' was successfully added.]')
	new_df_convo = df_convo.rename(index=str, columns=columns)
	#merge files
	df_temp = pd.concat([new_df_convo, df_all], sort=False)
	df_all = df_temp
#dict for listing the column
list_column = []
for i in columns.values():
	list_column.append(i)
#Add fieldname if there is no fieldname_rule in input_files
for i in list_field_rule:
	if i not in list_column:
		df_all[i] = ''
#print(list_column)
number = 0
print(' ')
print('		<<<<<<<<<<<<< converting fieldname >>>>>>>>>>>>>')
print('([old_fieldname_input] =>> [new_fieldname_input])')
for column in list_column:
	print(column, "=>>", list_column[number])
	number += 1

# customizing post_id, user_id, and caption
df_all['post_id'] = df_all['post_id'].apply(lambda x: str(x).replace('"','').replace("'",""))
df_all['user_id'] = df_all['user_id'].apply(lambda x: str(x).replace('"','').replace("'",""))
df_all['caption'] = df_all['caption'].apply(lambda x: str(x).replace('b"','').replace("b'","").replace('\n',' ').replace('\t',' ').replace('\r',' '))
df_all['caption'] = df_all['caption'].apply(lambda x: str(x).replace('"','').replace("'",""))
df_all['post_id'] = "'" + df_all['post_id']
df_all['user_id'] = "'" + df_all['user_id']
df_all['caption'] = "'" + df_all['caption'] 

fieldname_df_all = list(df_all)
if 'index' in fieldname_df_all:
	df_all = df_all.drop(columns=['index'])
df_all.reset_index()
df_all.index.names = ['index']
df_all.to_csv(output_name, index=True)
print('		[saved as ' + output_name+']')