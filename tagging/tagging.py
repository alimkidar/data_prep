import pandas as pd
import re


#------------------ CONFIGURATION -------------------------------
dict_filename = "tagging_library.csv"
input_filename = "convo.csv"


#load convo
convo = pd.read_csv(input_filename)


#nama kolom di "dict_filename"
head_keywords = 'keywords'
head_category1 = 'category1'
head_category2 = 'category2'
head_category3 = 'category3'
head_category_group = 'group_id'

#nama baris di "input_filename"
head_userid = 'user_id'
head_username = 'username'
head_caption = 'caption'
head_postid = 'post_id'
head_likes_count = 'like_count'
head_comment_count = 'comment_count'
head_brand = 'brand'
head_date_post = 'date_post'



#------------------ OUTPUT FILE -------------------------------
#1. tb_convo_count_percent.csv
#2. tb_pivot_percent.csv
#3. tb_user_statistics.csv
#4. tb_user_keywords.txt


#------------------	FUNCTION DEFINITIONS ------------------------
def filter_words(sentence):
	char = "!$%^&*()_-+=.?`~,ã:"

	for c in char:
		sentence = sentence.replace(c,' ')
	sentence = sentence.replace('"',' ')
	sentence = sentence.replace("\n"," ")
	sentence = sentence.replace("/"," ")
	sentence = sentence.replace("'"," ")
	sentence = sentence.replace("\\"," ")
	sentence = sentence.replace("  "," ")
	return sentence

#------------------ DATA LOADING --------------------------------

print ("Memulai")

#load dictionary dari file CSV yang bernama lib2.csv
mydict = {}
dfdict = pd.read_csv(dict_filename)
list_category_group_match =  []
list_category_group = {}
for index, row in dfdict.iterrows():
	keyword = str(row[head_keywords]).replace('"','').replace(",", "").replace("/"," ").replace("("," ").replace(")","").lower()
	category1 = str(row[head_category1]).lower()
	category2 = str(row[head_category2]).lower()
	category3 = str(row[head_category3]).lower()
	
	if len(category1) == 0:
		category1 = 'not-available'	
	if len(category2) == 0:
		category2 = 'not-available'
	if len(category3) == 0:
		category3 = 'not-available'

	category_group = str(row[head_category_group])
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
f6 = open(output_name6, "w", encoding="utf-8")
labelf6 = "Username,PostID,Conversation,Time Stamp,Keyword,ID_Group,Category1,Category2,Category3\n"
f6.write(labelf6)


hit = 0
hit_user = 1

#Data merupakan sebuah kumpulan kata secara temporary pada setiap caption (BERSIFAT TEMPORARY)
data = []

#perhitungan tiap post
post = []

#mencatat ID Group di setiap convo
post_id_group = {}
all_post_id_group = []

#perhitungan untuk semua post
user_post = {}

#data user list
user_list = []

# Mengolah Convo
for index, row in convo.iterrows():
	user_id = row[head_userid].replace('"', '').replace("'","")
	username = row[head_username].replace('"',"").replace(",","|").replace("'","")
	caption = filter_words(row[head_caption]).strip()
	post_id = row[head_postid].replace('"','').replace("'","")
	post_id_group[post_id] = []
	brand = row[head_brand]
	date_post = str(row[head_date_post])


	hit += 1
	if username not in user_list:
		user_list.append(username)

	#Untuk mencocokan keyword dalam caption
	for i in mydict:
		if i in caption:
			post.append(mydict[i])
			post_id_group[post_id].append(mydict[i]) #mydict[i] isinya IDGroup
			all_post_id_group.append(mydict[i])
			x_id_grup = mydict[i]

			cat1_ = list_category_group[x_id_grup][0]
			cat2_ = list_category_group[x_id_grup][1]
			cat3_ = list_category_group[x_id_grup][2]

			if type(caption) != bytes:
				caption = caption.encode('utf-8')
			caption = str(caption).replace("b'", "").replace("'","")
			#menulis CSV tentang keyword yang cocok dengan convo/caption yang kena
			isi = str(username) + ', "' + str(post_id) + '",' + str(caption)
			

			#labelf6 = "Username,PostID,Conversation,Time Stamp,Likes,Comments,Engagement,Keyword,ID_Group,Category1,Category2,Category3\n"
			id_group = str(x_id_grup)
			linef6 = (username + ",'" + post_id + "," + caption + "," + 
				date_post + "," + str(i) + "," + id_group + "," +  
				cat1_ + "," + cat2_ + "," + cat3_ + "\n")
			f6.write(linef6)
	print("Convo ke-" + str(hit) + " berhasil diolah.")
f6.close()
print("Data Tersimpan")
print("Proses Selesai")