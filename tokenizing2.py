print('Memulai')
#from nltk.collocations import *
#import nltk
from collections import Counter
import pandas as pd
import time

time_start = time.time()

# ----------------------- DEFINISI FUNGSI ------------------------------
def filter_words(sentence):
	char = "!$%^&*()_-+=.?`~,ã:"

	for c in char:
		sentence = sentence.replace(c,' ')
	sentence = sentence.replace('"',' ').replace("\n"," ").replace("/"," ").replace("'"," ").replace("\\"," ").replace("  "," ")
	return sentence

def stopword(sentence, file_csv, column):
	dfsw = pd.read_csv(file_csv)
	for index, row in dfsw.iterrows():
		sentence = sentence.replace(row[column]," ")
	return sentence

def anti_hm(word_):
	if '#' in word_ or '@' in word_:
		word_clean = word_.replace('@','').replace('#','')
	else:
		word_clean = word_
	return word_clean

def anti_space(word_):
	while ' ' in word_:
		word_ = word_.replace(' ','')
	return word_
def quantilizing_5(df_input, fieldname_input, fieldname_output):
	df_input_quantile = df_input.quantile([0.2,0.4,0.6,0.8])
	q1 = df_input_quantile.iloc[0][fieldname_input]
	q2 = df_input_quantile.iloc[1][fieldname_input]
	q3 = df_input_quantile.iloc[2][fieldname_input]
	q4 = df_input_quantile.iloc[3][fieldname_input]

	df_input[fieldname_output] = df_input[fieldname_input].apply(lambda x: 1 if (x < q1) else 2 if (q1 <= x < q2) else 3 if (q2 <= x < q3) else 4 if (q3 <= x < q4) else 5 if ( x >= q4) else 'Error')
	return df_input

def find_column(word, df):
	for i in list(df):
		if word in i.lower():
			x = i
		else:
			pass
	return x
def quantilizing_2(df_input, fieldname_input, fieldname_output):
	df_input_quantile = df_input.quantile([0.8])
	q1 = df_input_quantile.iloc[0][fieldname_input]

	df_input[fieldname_output] = df_input[fieldname_input].apply(lambda x: 1 if (x > q1) else 2 if (x <= q1) else 'Error')
	return df_input
	
print('Load Convo')



# ----------------------- LOAD DATA ------------------------------
dfconvo_raw = pd.read_csv('output/convo_high.csv')
dfdict = pd.read_csv('config/lib_keywords.csv')

column_brand = find_column('brand', dfconvo)
list_brands = list(set(list(dfconvo[column_brand])))

df_dirty_word_count_1000_all = pd.DataFrame()
df_clean_word_count_150_all = pd.DataFrame()
df_clean_word_include_100_all = pd.DataFrame()
df_brn_input_all = pd.DataFrame()
df_brn_input_filtered_200_all = pd.DataFrame()


class main():
	def __init__(self, brand):
		dfconvo = dfconvo_.loc[dfconvo_[column_brand] == brand]
		df_dirty_word = pd.DataFrame()
		df_dirty_word_bigram = pd.DataFrame()
		df_clean_word = pd.DataFrame()
		df_word_extraction = pd.DataFrame()
		df_brn_input = pd.DataFrame()

		# Load stopword
		'''f = open('words.txt', 'r')
		content = f.read()
		stopwords = content.split(',')'''

		bigram = {}

		hit = 0
		hitung = 0

		#Keyword and Alias KEBALIK YA
		dictionary = {}
		dictionary_bigram = {}

		#List dari alias
		list_alias = []
		list_alias_unigram = []
		list_alias_bigram = []
		list_non_alias = []


		#list_dirty_word
		list_dirty_word_list = []
		list_clean_word_list = []

		print('Load Convo Completed')

		dict_word_extraction = []

		# ----------------------- LOAD KEYWORD AND ALIAS  ------------------------------
		# Looping untuk memisah unigram dan bigram dari keyword
		for index, row in dfdict.iterrows():
			keyword = row['keywords'].lower()
			alias =  row['alias'].lower()
			if ' ' in keyword:
				if alias not in dictionary_bigram:
						dictionary_bigram[alias] = keyword
			else:
				if keyword not in dictionary:
						dictionary[keyword] = alias

		# ----------------------- MENGOLAH DATA ------------------------------
		# Looping pengolahan convo
		for index, row in dfconvo.iterrows():
			list_alias_per_convo = []
			conversation_raw = str(row['caption']).lower()
			conversation = str(row['caption']).lower()
			postid = row['post_id']

			#Hasgtag Mention Ectractor
			caption_clean = filter_words(conversation).replace('#',' #').replace('@', ' @').replace('%','')
			words = caption_clean.split()
			for word in words:
				if len(word.replace('#','').replace('@','')) != 0:
					if '#' in word:
						a_clean = word.replace("#","")
						a = word

						if a_clean in dictionary:
							list_alias.append(dictionary[a_clean])
							list_alias_per_convo.append(dictionary[a_clean])
						else:
							list_alias.append(a)
							list_alias_per_convo.append(a)
						conversation = conversation.replace(word,' ')

					if '@' in word:
						a_clean = word.replace("@","")
						a = word
						if a_clean in dictionary:
							list_alias.append(dictionary[a_clean])
							list_alias_per_convo.append(dictionary[a_clean])
						else:
							list_alias.append(a)
							list_alias_per_convo.append(a)
						conversation = conversation.replace(word,' ')

			tokens_filter = filter_words(conversation)
			#Kode dibawah untuk pake stopword
			#tokens_stopword = stopword(conversation, 'stopwords.csv', 'words')

			# tokens = convo yang sudah di tokenize menjadi UNIGRAM (list)
			# bigrams = convo yang sudah di pecah menjadi BIGRAM (list)
			tokens = tokens_filter.split()

			#BIGRAM and more-gram!!!!!!!!!!!!!
			# without space
			text = anti_space(conversation)
			for alias in dictionary_bigram:
				keyword = dictionary_bigram[alias]
				keyword_nospace = anti_space(keyword)
				if keyword_nospace in text:
					list_alias_per_convo.append(alias)
					list_alias.append(alias)
					list_alias_bigram.append(alias)

			# ----------------------------------------------
			#	DICTIONARY UNIGRAM DAN BIGRAM TERBALIK!!!!
			#dictionary_bigram[alias] = keyword
			#dictionary[keyword] = alias
			# ----------------------------------------------
			for keyword in dictionary:
				if keyword in tokens:
					list_alias_per_convo.append(dictionary[keyword])
					list_alias.append(dictionary[keyword])
					list_alias_unigram.append(dictionary[keyword])
					#print(keyword, '=>',  dictionary[keyword])

			for i in range(len(tokens)):
				list_dirty_word_list.append(tokens[i])
				#word = tokens[i]
				#if word not in stopwords and len(word) > 2:
				#	list_dirty_word_list.append(word)
			bigram_all = {}
			hitung = 0

			#Membuat bigram
			anti_duplicate = []
			for	w1 in list_alias_per_convo:
				for w2 in list_alias_per_convo:
					if w1 != w2:
						w1 = anti_hm(w1)
						w2 = anti_hm(w2)
						if w1 + w2 not in anti_duplicate:
							anti_duplicate.append(w1 + w2)
							anti_duplicate.append(w2 + w1)
							#labelf = 'conversation,w1,w2,w1w2\n'
							#linef = conversation_raw.replace(",",".").replace("\n"," ").replace("\r"," ") + "," + w1 + "," + w2 + "," + w1 + w2 + "\n"
							#f.write(linef)
							dict_word_extraction.append({'1_conversation': conversation_raw, '2_w1': w1, '3_w2': w2, '4_w1w2': w1 + w2})
							#df_word_extraction = df_word_extraction.append(pd.DataFrame([[conversation_raw, w1, w2, w1 + w2]],columns=['conversation','w1','w2','w1w2']))
			print('convo-' + str(hit) + ' ' + str(time.time() - time_start) + ' detik.' )
			hit += 1


		# ----------------------- PRINT INTO CSV FILE ------------------------------
		# DATAFRAME TRULY KOTOR: Dirty means all words in convo
		#df_dirty_word['word'] = pd.Series(list_dirty_word_list).values
		#stopword

		df_dirty_word = pd.DataFrame.from_dict(list_dirty_word_list)
		df_dirty_word.columns = ['word']
		# DATAFRAME MATCH KEYWORD
		df_clean_word_raw = pd.DataFrame.from_dict(list_alias)
		df_clean_word_raw.columns = ['word']

		# Membuat CSV DIRTY WORD (@# exclude)
		df_dirty_word_count = pd.DataFrame(df_dirty_word['word'].apply(lambda x: x.replace("#","").replace("@","") if ("#" in x or "@" in x) else x))
		df_dirty_word_count = df_dirty_word_count.groupby(['word']).size().reset_index(name='counts')
		df_dirty_word_count_1000 = df_dirty_word_count.nlargest(1000, 'counts')
		
		df_dirty_word_count_1000_all = pd.concat([df_dirty_word_count, df_dirty_word_count_1000])
		print('dirty_word_1000.csv')

		# Membuat CSV Clean Word (@# exclude) ============================================
		df_clean_word_count = pd.DataFrame(df_clean_word_raw['word'].apply(lambda x: x.replace("#","").replace("@","") if ("#" in x or "@" in x) else x))
		df_clean_word_count = df_clean_word_count.groupby(['word']).size().reset_index(name='counts')
		df_clean_word_count_150 = df_clean_word_count.nlargest(150, 'counts')
		df_clean_word = quantilizing_5(df_clean_word_count_150,'counts','class')
		
		df_clean_word_count_150_all = pd.concat([df_clean_word_count_150_all, df_clean_word_count_150])
		print('clean_word_150.csv')


		# Membuat CSV Clean Word (@# include)
		df_clean_word_include = df_clean_word_raw.groupby(['word']).size().reset_index(name='counts')
		df_clean_word_include_100 = df_clean_word_include.nlargest(100, 'counts')
		df_clean_word_include_100['note'] = df_clean_word_include_100['word'].apply(lambda x: 'hashtag' if ("#" in x) else 'mention' if ("@" in x) else 'keyword')
		
		df_clean_word_include_100_all = pd.concat([df_clean_word_include_100_all, df_clean_word_include_100])

		print('Membuat CSV clean_word_100_include.csv Selesai. ' + str(time.time() - time_start) + ' detik.' )

		# MEMBUAT CSV CLEAN WORD
		df_list_alias = pd.DataFrame.from_dict(list_alias)
		list_alias_bersih = [str(s).strip('@').strip('#') for s in list_alias]
		counter = Counter(list_alias_bersih)
		df_w = pd.DataFrame.from_dict(counter, orient='index').reset_index()

		#MULAI LAMA!
		df_word_extraction = pd.DataFrame.from_dict(dict_word_extraction)
		print('dict to df')
		df_word_extraction_count = df_word_extraction.groupby(['4_w1w2','2_w1','3_w2']).size().reset_index(name='counts')
		print('hitung count')
		# Loop perhitungan Edge BRN
		dict_brn_input = []
		hit_loop_brn = 0
		print('mulai loop')

		for index, row in df_word_extraction_count.iterrows():
			w1 = row['2_w1']
			w2 = row['3_w2']
			w1w2 = row['4_w1w2']
			w1w2_counts = int(row['counts'])
			w1_counts = int(counter[w1])
			w2_counts = int(counter[w2])
			edge = (w1_counts * w2_counts) / w1w2_counts
			dict_brn_input.append({'1_w1': w1, '2_w1_counts': w1_counts, '3_w2': w2, '4_w2_counts':w2_counts, '5_w1w2': w1w2, '6_w1w2_counts': w1w2_counts, '7_edge': edge })
			
			print('loop ke ' + str(hit_loop_brn) + ' ' + str(time.time() - time_start) + ' detik.', str(w1), '=', str(w1_counts), str(w2), '=', str(w2_counts))
			hit_loop_brn += 1
			#df_brn_input = df_brn_input.append(pd.DataFrame([[ w1, w1_counts, w2, w2_counts, w1w2, w1w2_counts, edge]],columns=['w1', 'w1_counts','w2', 'w2_counts','w1w2', 'w1w2_counts','edge'])) 
		print('Loop selesai')
		df_brn_input = pd.DataFrame.from_dict(dict_brn_input)
		df_brn_input.columns = ['w1', 'w1_counts', 'w2', 'w2_counts', 'w1w2', 'w1w2_counts', 'edge']

		#quantile
		df_brn_input = quantilizing_2(df_brn_input,'w1_counts','class_w1')
		df_brn_input = quantilizing_2(df_brn_input,'w2_counts','class_w2')
		df_brn_input = quantilizing_2(df_brn_input,'w1w2_counts','class_w1w2')
		df_brn_input = quantilizing_2(df_brn_input,'edge','class_edge')
		df_brn_input_all = pd.concat([df_brn_input_all, df_brn_input])

		df_brn_input_filtered = df_brn_input[(df_brn_input.class_w1 == 1) & (df_brn_input.class_w2 == 1) & (df_brn_input.class_w1w2 == 1) & (df_brn_input.class_edge == 1)]


		df_brn_input_filtered_200 = df_brn_input_filtered.nlargest(5000, 'edge')
		df_brn_input_filtered_200 = quantilizing_5(df_brn_input_filtered_200,'w1_counts','normalisasi_w1_counts')
		df_brn_input_filtered_200 = quantilizing_5(df_brn_input_filtered_200,'w2_counts','normalisasi_w2_counts')
		df_brn_input_filtered_200 = quantilizing_5(df_brn_input_filtered_200,'w1w2_counts','normalisasi_w1w2_counts')
		df_brn_input_filtered_200 = quantilizing_5(df_brn_input_filtered_200,'edge','normalisasi_edge')
		df_brn_input_filtered_200_all = pd.concat([df_brn_input_filtered_200_all, df_brn_input_filtered_200])
		print('Membuat CSV BRN Selesai. ' + str(time.time() - time_start) + ' detik.' )

for i in list_brands:
	main(i)
	print(str(i), 'Done.')

df_brn_input_filtered_200_all.to_csv('output/BRN_Input.csv')
df_brn_input_all.to_csv('output/BRN_Input_kotor.csv')
df_dirty_word_count_1000_all.to_csv('output/dirty_word_1000.csv')
df_clean_word_count_150_all.to_csv('output/clean_word_150.csv')
df_clean_word_include_100_all.to_csv('output/clean_word_100_include.csv')