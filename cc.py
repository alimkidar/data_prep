import pandas as pd
import numpy as np
import datetime
from logger import Logger
time_now = str(int(datetime.datetime.now().timestamp()))

def is_in(keyword, string, value_if_true, value_if_false):
	x = ''
	y = string.lower()
	keyword = keyword.lower()
	text = "".join(y.split())
	key = "".join(keyword.split())
	if key in text:
		x = value_if_true
	else:
		x = value_if_false
	return x
def five_words(text):
	output = ''
	token = str(text).lower().split()
	if len(token) > 4:
		output = token[0] + token[1] + token[2] + token[3] + token[4]
	else:
		output = ''.join(token)
	return output

def contextualisation(value, if_true, if_false):
	x = ''
	if value >= 0:
		x = if_true
	else:
		x = if_false
	return x
def count_x(x, text):
	# fixing double x in a string(ex: @@me, ###motivation)
	a = text.replace(x, ' ' + x)
	q = a.replace(x + ' ',' ')
	count = q.count(x)
	return count

def load_to_list(file_, column_key, column_detail, value_detail):
	dfx = pd.read_csv(file_)
	dfy = dfx.loc[dfx[column_detail] == value_detail]
	x = dfy[column_key].tolist()
	return x

#Load conversation and dictionary
df_convo = pd.read_csv('output/convo.csv',dtype=str, low_memory=False)
df_dic = pd.read_csv('config/lib_keywords.csv')
fieldname_raw = list(df_convo)

#build dictionary
df_dic['keywords'] = df_dic['keywords'].str.lower()
df_dic.set_index('keywords', drop=True, inplace=True)
dictionary = df_dic.to_dict(orient='inplacendex')

dirtywords = load_to_list('config/lib_dirtywords.csv', 'words', 'keterangan', 'dirtyword')
excludes = load_to_list('config/lib_dirtywords.csv', 'words', 'keterangan', 'exclude')
print('excludes')

# delete exclude words
for x in excludes:
	df_convo = df_convo.replace({'caption':{x: ''}}, regex=True)
print('dictionary')

# contextual checking and dirtyword checking
df_convo['dirtywords'] = ''
df_convo['score'] = 0
for key in dictionary:
	df_convo[key] = np.vectorize(is_in)(key, df_convo['caption'], int(dictionary[key]['score']), 0)
print('dirtyword')
for dirtyword in dirtywords:
	df_convo[dirtyword] = np.vectorize(is_in)(dirtyword, df_convo['caption'], -12, 0)
	df_convo['dirtywords'] = np.vectorize(is_in)(dirtyword, df_convo['caption'], df_convo['dirtywords'] + dirtyword + "|", df_convo['dirtywords'])
print('x')

#get sum of all
hidden_fieldname = list(dictionary) + list(dirtywords)
df_convo['score'] = df_convo[hidden_fieldname].sum(1)


#generate new id. ID = dattabot_USERNAME_DATEPOST_5FISRTWORDSCAPTION
df_convo['id_dattabot'] = 'dattabot_' + df_convo['username'].apply(str) + '_' + df_convo['timestamp'].apply(str) + '_' + five_words(df_convo['caption'])

#df_convo['contextual'] = contextualisation(df_convo['score'].any(), 'Yes', 'No')
df_convo['contextual'] = np.vectorize(contextualisation)(df_convo['score'], 'Yes', 'No') 

df_convo['mention'] =  np.vectorize(count_x)('@', df_convo['caption'])
df_convo['hashtag'] =  np.vectorize(count_x)('#', df_convo['caption'])
df_convo['QC'] = ''

# re-order fieldname
try:
	fieldname_raw.remove('caption')
	fieldname = ['id_dattabot','caption', 'score', 'contextual','dirtywords', 'hashtag', 'mention', 'QC']
	fieldname.extend(fieldname_raw)
	df_convo_output = df_convo.drop(hidden_fieldname, axis=1)
	df_convo_output = df_convo_output[fieldname]
except:
	raise
log_name = 'convo_' + time_now + '.csv'
df_convo_output.to_csv('output/convo.csv')
df_convo_output.to_csv('temp/'+log_name)
Logger("[" + time_now + " add {'convo.csv'} into " + log_name+"]\n").write_log()

print('DONE!')