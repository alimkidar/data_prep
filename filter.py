import pandas as pd
import numpy as np
import datetime
from logger import Logger
time_now = str(int(datetime.datetime.now().timestamp()))

# to load csv file
def load_csv(file):
	try:
		df = pd.read_csv(file, dtype=str, low_memory=False)
	except:
		print('Error: load file!')
		raise	
	return df

# to fill blank value from column x to column y
def fill_blank(df, x, y):
	try:
		x = np.vectorize(x_to_y)(x, y)
	except:
		print("Error: there is no 'QC' in fieldnames!")
		raise	
	return df

def x_to_y(x, y):
	if y == '':
		a = x
	else:
		a = y
	print(a)
	return a

# to select data by value in particular column
def select(df, column, value):
	try:
		df = df.loc[df[column] == value]
	except:
		print("Error: there is no 'QC' in fieldnames!")
		raise
	return df	
	# save dataframe to csv
def s_to_csv(df, filename):
	try:
		df.to_csv(filename)
	except:
		print("Error: can't save file!")
		raise
# aggregate score convos by fildname
def aggregate_by(df, fieldname_group, fieldname_value):
	try:
		fieldname_group = str(fieldname_group)
		fieldname_value = str(fieldname_value)
		df = df.groupby([fieldname_group])[fieldname_value].sum()
		df.columns = [fieldname_group, fieldname_value]
		df.to_csv('temp/account_dict_' + time_now + '.csv')
	except:
		print('Error: aggregate value!')
		raise
	return 

df = load_csv('output/convo.csv')
df = df.fillna('')
df['QC'] = np.vectorize(x_to_y)(df['contextual'], df['QC'])

#df = fill_blank(df, df['contextual'], df['QC'])
df = select(df, 'QC','yes')
print(df.head())
s_to_csv(df, 'output/convo_high.csv')
s_to_csv(df, 'temp/convo_high_' + time_now + '.csv')
df = select(df, 'QC','no')#
s_to_csv(df, 'temp/convo_low_' + time_now + '.csv')
df = aggregate_by(df, 'username','score')
log = str('['+ time_now + "{'convo.csv'}" + ' has been filtered with TIMESTAMP: '+ time_now +']\n')
Logger(log).write_log()

print('Done!')
'''
class QC_Filter():
	def __init__(self):
		print('Init')
		self.load_csv()
		self.df = self.df.fillna('')
		self.fill_blank(self.df['contextual'], self.df['QC'])
		self.select('QC','yes')
		self.s_to_csv('output/convo_high.csv')
		self.s_to_csv('temp/convo_high_' + time_now + '.csv')
		self.select('QC','no')
		self.s_to_csv('temp/convo_low_' + time_now + '.csv')
		self.aggregate_by('username','score')
		log = str('['+ time_now + "{'convo.csv'}" + ' has been filtered with TIMESTAMP: '+ time_now +']\n')
		Logger(log).write_log()

		print('Done!')

	# to load csv file
	def load_csv(self):
		try:
			self.df = pd.read_csv('output/convo.csv', dtype=str, low_memory=False)

		except:
			print('Error: load file!')
			raise	

	# to fill blank value from column x to column y
	def fill_blank(self, x, y):
		try:
			x = np.vectorize(self.x_to_y)(x, y)
		except:
			print("Error: there is no 'QC' in fieldnames!")
			raise	
	def x_to_y(self, x, y):
		if y == '':
			y = x
		return y

	# to select data by value in particular column
	def select(self, column, value):
		try:
			df = self.df
			self.df_select = df.loc[df[column] == value]
		except:
			print("Error: there is no 'QC' in fieldnames!")
			raise	
	# save dataframe to csv
	def s_to_csv(self, filename):
		try:
			self.df_select.to_csv(filename)
		except:
			print("Error: can't save file!")
			raise

	# aggregate score convos by fildname
	def aggregate_by(self, fieldname_group, fieldname_value):
		try:
			fieldname_group = str(fieldname_group)
			fieldname_value = str(fieldname_value)
			self.df_a = self.df.groupby([fieldname_group])[fieldname_value].sum()
			self.df_a.columns = [fieldname_group, fieldname_value]
			self.df_a.to_csv('temp/account_dict_' + time_now + '.csv')
		except:
			print('Error: aggregate value!')
			raise
QC_Filter()'''