import pandas as pd
import re
from collections import Counter

#------------------ CONFIGURATION -------------------------------
dict_filename = "tagging_library.csv"
input_filename = "convo.csv"

#load convo
convo = pd.read_csv(input_filename)

# Fieldname in (dict_filename)
head_keywords = 'keywords'
head_category1 = 'category1'
head_category2 = 'category2'
head_category3 = 'category3'

# This script need this fieldname from input file (input_fieldname):
head_username = 'username'
head_caption = 'caption'
head_postid = 'post_id'
head_timestamp = 'timestamp'

#------------------ OUTPUT FILE -------------------------------
#1. Tagging_result.csv
#2. Unlisted.csv

#------------------	FUNCTION DEFINITIONS ------------------------
def filter_words(sentence):
	char = "$^&-+=.?`~,ã:"
	for c in char:
		sentence = sentence.replace(c,' ')
	sentence = sentence.replace('"',' ')
	sentence = sentence.replace("\n"," ")
	sentence = sentence.replace("'"," ")
	sentence = sentence.replace("\\"," ")
	sentence = sentence.replace("  "," ")
	sentence_lower = sentence.lower()
	return sentence_lower

#------------------ LOADING DATA ----------------------------
print ("Memulai")
#load dictionary file (dict_filename)
mydict = {}
dfdict = pd.read_csv(dict_filename)
list_category_group_match =  []
list_category_group = {}
for index, row in dfdict.iterrows():
	keyword = str(row[head_keywords]).lower()
	category1 = str(row[head_category1])
	category2 = str(row[head_category2])
	category3 = str(row[head_category3])
	
	if len(category1) == 0:
		category1 = 'not-available'	
	if len(category2) == 0:
		category2 = 'not-available'
	if len(category3) == 0:
		category3 = 'not-available'
	#concating categories into a string
	category_group = category1 + category2 + category3
	if category_group not in list_category_group:
		list_category_group[category_group] = []

		list_category_group[category_group].append(category1)
		list_category_group[category_group].append(category2)
		list_category_group[category_group].append(category3)
	mydict[keyword] = category_group
	list_category_group_match.append(category_group)
list_category_group_match = list(set(list_category_group_match))

#Seting Output6 ====================================================================
output_name6 = "tagging_result.csv"
f = open(output_name6, "w", encoding="utf-8")
label = "username,post_id,caption,timestamp,keyword,category1,category2,category3\n"
f.write(label)

hit = 0
hit_user = 1

#temporary data for every matched keyword in every caption
data = []

#for counting posts
post = []

#for listing  group_id
post_id_group = {}
all_post_id_group = []

#for counting all posts
user_post = {}

#for listing username
user_list = []

dict_unlisted_hashtag = []

#Get data from Fieldname: username, caption, timestamp, post_id
for index, row in convo.iterrows():
	username = str(row[head_username]).replace(",","|")
	caption_raw = row[head_caption]
	caption_ = caption_raw.replace('#', ' #')
	caption = filter_words(caption_).strip()
	post_id = str(row[head_postid])
	post_id_group[post_id] = []
	timestamp = str(row[head_timestamp])
	
	#encode caption
	if type(caption) != bytes:
		caption = caption.encode('utf-8')
	caption = str(caption).replace("b'", "").replace("'","")
	# get only last 1000 char
	if len(caption) > 30000:
		x = len(caption) - 1000
		caption = caption[x:]

	#Parsing #(hashtag) from caption
	token = caption.split()
	token_hashtag = []
	for word in token:
		if '#' in word and len(word) > 1:
			hashtag_temp = word.replace('#','')
			token_hashtag.append(hashtag_temp)
		else:
			pass
	#listing username into a list
	if username not in user_list:
		user_list.append(username)

	#Extracting hashtag which is not be found in library
	for hashtag in token_hashtag:
		hashtag = hashtag.replace('#','')
		if hashtag not in mydict:
			dict_unlisted_hashtag.append(hashtag)		
	
	#Searching keywords in every caption (conversation)
	for keyword in mydict:
		if keyword in caption:
			post.append(mydict[keyword])
			post_id_group[post_id].append(mydict[keyword]) #mydict[i] isinya IDGroup
			all_post_id_group.append(mydict[keyword])
			x_id_grup = mydict[keyword]

			cat1_ = list_category_group[x_id_grup][0]
			cat2_ = list_category_group[x_id_grup][1]
			cat3_ = list_category_group[x_id_grup][2]

			#print into CSV file if keyword in caption
			isi = str(username) + ', "' + str(post_id) + '",' + str(caption)
			
			#fieldname output = username | post_id | caption | timestamp | keyword | category1 | category2 | category3
			id_group = str(x_id_grup)
			linef6 = (username + ",'" + post_id + "," + caption + "," + 
				timestamp + "," + str(keyword) + "," +  
				cat1_ + "," + cat2_ + "," + cat3_ + "\n")
			f.write(linef6)
		else:
			pass
	hit += 1
	print("Convo ke-" + str(hit) + " . Caption: " + caption)
f.close()

# Counting ublisted hashtag
dict_unlisted_hashtag_count = Counter(dict_unlisted_hashtag)

# Converting unlisted hashtag dataframe into csv file
df_unlisted_hashtag = pd.DataFrame.from_dict(dict_unlisted_hashtag_count,orient='index').reset_index()
df_unlisted_hashtag.columns = ['hashtag','count']
df_unlisted_hashtag.to_csv('unlisted.csv')
print("Data Tersimpan")
print("Proses Selesai")