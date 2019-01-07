print ("Memulai")
from collections import Counter
import pandas as pd
import requests
import json
import re


#------------------ CONFIGURATION -------------------------------
dict_filename = "interest_library.csv"
input_filename = "convo.csv"


#load convo
convo = pd.read_csv(input_filename)

#nama kolom di "dict_filename"
head_keywords = 'keywords'
head_interest = 'sub_interest'

#nama baris di "input_filename"
head_userid = 'user_id'
head_username = 'username'
head_caption = 'caption'
head_postid = 'post_id'
head_likes_count = 'like_count'
head_comment_count = 'comment_count'

#------------------	FUNCTION DEFINITIONS ------------------------
def hitung_persen(nilai, total):
	try:
		x =  nilai / total
	except:
		x = 0
	return x

def delete_petik(word):
	word_x = word
	if type(word) != int:
		word_x = word.replace('"','').replace("'","")
	return word_x


#------------------ DATA LOADING --------------------------------
#load dictionary dari file CSV yang bernama lib2.csv
mydict = {}
dfdict = pd.read_csv(dict_filename)
list_interest =  []
for index, row in dfdict.iterrows():
	keyword = str(row[head_keywords]).replace('"','').replace(",", "").replace("/"," ").replace("("," ").replace(")","").lower()
	interst = str(row[head_interest])
	

	mydict[keyword] = interst
	list_interest.append(interst)
list_interest = list(set(list_interest))
str_interest = ""
str_sumof_interest = ""

#List jumlah interest (dapat menyesuaikan, sesuai lib2.csv)
for i in list_interest:
	str_interest = str_interest + "," + i
	str_sumof_interest = str_sumof_interest + "," + i 


#Seting Output2 =============================
output_name2 = "profiling_result.csv"
g = open(output_name2, "w")
label2 = "Username" + str_sumof_interest + "\n"
g.write(label2)

#Seting Output4 =============================
output_name4 = "profiling_convo.csv"
z = open(output_name4, "w")
label4 = "Username,PostID,Caption,Keyword,Sub_Interest\n"
z.write(label4)

hit = 1
hit_user = 1

#Data merupakan sebuah kumpulan kata secara temporary pada setiap caption (BERSIFAT TEMPORARY)
data = []


#perhitungan tiap post
post = []

#perhitungan untuk semua post
user_post = {}

#data user list
user_list = []


#Pencatatan. Hasilnya nantinya akan | count_user_posts['username'] = jumlah post |
count_user_posts = {}
count_user_likes = {}
count_user_comments = {}
count_user_followers = {}
count_user_following = {}

for index, row in convo.iterrows():
	user_id = delete_petik(row[head_userid])
	username = row[head_username]
	caption = str(row[head_caption]).replace(",","").replace(r"\u","|").replace("\n","|").replace("\r","|").lower().strip()
	post_id = delete_petik(row[head_postid])

	print (str(hit) + ". " + username + ": " + caption)
	print(" ")
	hit += 1
	if username not in user_list:
		user_list.append(username)


	#Mengecek apakah username sudah masuk di dalam count_user_post
	if username not in count_user_posts:
		count_user_posts[username] = 1
	else: 
		count_user_posts[username] += 1

	if username not in count_user_likes:
		count_user_likes[username] = row[head_likes_count]
	else:
		count_user_likes[username] += row[head_likes_count]

	if username not in count_user_comments:
		count_user_comments[username] = row[head_comment_count]
	else:
		count_user_comments[username] += row[head_comment_count]

	#Untuk mencocokan keyword dalam caption
	for i in mydict:
		if i in caption:
			post.append(mydict[i])
			#menulis CSV tentang keyword yang cocok dengan convo/caption yang kena
			isi = (username + ',-' + str(post_id) + ',' + str(caption) + "," + str(i) + "," + str(mydict[i]))
			if type(isi) != bytes:
				isi = isi.encode('utf-8')

			isi = str(isi).replace("b'","").replace("'","") + "\n"
			isi = isi.replace("-",'"')
			z.write(isi)

	#Sebagai counter. atau perhitungan jumlah berapa interest nya (travel, berapa culinary, berapa musik, dll)
	counter = Counter(post)
	post = []
	total_keyword = sum(counter.values())
	

	#Mengambil nilai dari setiap interestnya, dan menyimpannya dalam satu data list
	nilai_interets = {}
	str_nilai_interest = ""
	for i in list_interest:
		nilai_interets[i] = hitung_persen(counter[i], total_keyword)

	if username not in user_post:
		user_post[username] = {}

	#menghitung jumlah interest dari masing2 user (semua post)
	for i in list_interest:
		if str(i) not in user_post[username]:
			user_post[username][i] = 0
		user_post[username][i] += nilai_interets[i]

for i in user_list:
	#i = username
	str_nilai_interest_user = ""
	for j in user_post[i]:
		 str_nilai_interest_user = str_nilai_interest_user + "," + str(user_post[i][j])

	#total_percent = user_post[i]['traveling'] + user_post[i]['fashion'] + user_post[i]['culinary'] + user_post[i]['music']
	total_percent = sum(user_post[i].values())
	str_percent_total = ""
	percent_total = {}
	for j in list_interest:
		percent_total[j] = hitung_persen(user_post[i][j], total_percent)
		str_percent_total = str_percent_total + "," + str(percent_total[j])

	#Header = "Username,Sum of interest"
	g.write(str(i) + str_percent_total + "\n")


g.close()
z.close()
print("Data Tersimpan")
print("Proses Selesai")
