import os
from os import listdir
import pandas as pd
import numpy as np

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
	return text_


#Load Rules
df_rules = pd.read_csv('rules.csv')
dict_contain = {}
dict_not_contain = {}
fieldname_list = []
for index, row in df_rules.iterrows():
	fieldname = row['fieldname']
	contain = no_space(row['contain'])
	not_contain = no_space(row['not_contain'])

	#Parsing by slash /
	slash_contain = contain.split('/')
	slash_not_contain = not_contain.split('/')

	#Add to dict
	dict_contain[fieldname] = slash_contain
	dict_not_contain[fieldname] = slash_not_contain
	

	if fieldname not in fieldname_list:
		fieldname_list.append(fieldname)

#===============================Rules end=================================

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
	print(' ')


fish_contain = False
fish_not_contain = False
list_field_rule = []
output_name = "convo_result.csv"
#setiap csv
for csv in all_csv:
	columns = {} #columns['lama'] = 'baru'
	df_convo = pd.read_csv(csv, index_col=False)
	headers = list(df_convo)
	follower_status = False
	# untuk setiap fieldinput di csv input
	print('			[Converting ' + csv + ']')
	for field_input_ in headers:
		field_input = field_input_.lower()
		#untuk setiap fieldrule di csv rule
		for field_rule_ in dict_contain.keys():
			field_rule = field_rule_.lower()
			if field_rule_ not in list_field_rule:
				list_field_rule.append(field_rule_)
			contain_rules_split_ = dict_contain[field_rule_]
			for rule in contain_rules_split_:
				contain_rules_split = rule.split('+')
				rule_count = len(contain_rules_split)
				for word in contain_rules_split:
					if word.lower() in field_input:
						rule_count -= 1
				if rule_count > 0:
					fish_contain = False
				else:
					fish_contain = True

				#fish_contain = all(x in field_input for x in contain_rules_split)
				#fish_contain = all(word in field_input for word in contain_rules_split)
			not_contain_rules_split_ = dict_not_contain[field_rule_]
			for rule in not_contain_rules_split_:
				not_contain_rules_split = rule.split('+')
				rule_count = len(not_contain_rules_split)
				for word in not_contain_rules_split:
					if word.lower() not in field_input_:
						rule_count -= 1
				if rule_count > 0:
					fish_not_contain = False
				else:
					fish_not_contain = True

			#fish_not_contain = all(x in field_input for x in not_contain_rules_split)
			if fish_contain == True and fish_not_contain == True:
				columns[field_input] = field_rule_
				print(field_input + ' => ' + field_rule_)
		if field_input not in columns:
			columns[field_input] = field_input
			
	print(columns)

	new_df_convo = df_convo.rename(index=str, columns=columns)
	new_df_convo = new_df_convo[pd.notnull(new_df_convo['post_id'])]
	df_temp = pd.concat([new_df_convo, df_all], sort=False)
	df_all = df_temp
	
list_column = []
for i in columns.values():
	list_column.append(i)
df_all[list_column].to_csv(output_name)
print('		[saved as ' + output_name+']')
'''


			for word in contain_rules_split:
				if word in field_input:
					fish = True



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

	df_temp = pd.concat([new_df_convo, df_all])
	df_all = df_temp
	print('Converting ' + csv)

df_all[list_column].to_csv(output_name)

print('Done')

'''