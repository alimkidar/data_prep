print ("Initializing...")
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

# define fieldname "input_filename"
head_userid = 'user_id'
head_username = 'username'
head_caption = 'caption'
head_postid = 'post_id'
head_likes_count = 'like_count'
head_comment_count = 'comment_count'

#------------------	FUNCTION DEFINITIONS ------------------------
def count_percent(nilai, total):
	try:
		x =  nilai / total
	except:
		x = 0
	return x

def no_quote(word):
	word_x = word
	if type(word) != int:
		word_x = word.replace('"','').replace("'","")
	return word_x
def clean_word(word):
	word = word.replace('b"','').replace("b'","").replace('\r','').replace("\n","").replace('\t','').replace('\\','').replace(',','|')
	word = word.replace('"','').replace("'","")
	return word


#------------------ LOADING DATA --------------------------------
#load library
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

# customizung the interest (based on library unix interest)
for i in list_interest:
	str_interest = str_interest + "," + i
	str_sumof_interest = str_sumof_interest + "," + i 


# Output 1 =============================
output_name2 = "profiling_result.csv"
g = open(output_name2, "w")
label2 = "username" + str_sumof_interest + "\n"
g.write(label2)

#Output 2 =============================
output_name4 = "profiling_convo.csv"
z = open(output_name4, "w")
label4 = "username,post_id,caption,keyword,sub_interest\n"
z.write(label4)

hit = 1
hit_user = 1

# dict for collecting temporary match word from the caption
data = []
post = []
user_post = {}
user_list = []

# store data post, like, following, and follower for every user. XXX['username'] = total posts/likes/comment
count_user_posts = {}
count_user_likes = {}
count_user_comments = {}
count_user_followers = {}
count_user_following = {}

for index, row in convo.iterrows():
	user_id = no_quote(row[head_userid])
	username = clean_word(str(row[head_username]))
	caption = str(row[head_caption]).replace(",","").replace(r"\u","|").replace("\n","|").replace("\r","|").replace("\t","|").lower().strip()
	post_id = no_quote(row[head_postid])

	hit += 1
	if username not in user_list:
		user_list.append(username)

	# listing total post/comment/likes into a dict
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

	# string matching [whether keyword in caption = True | nor in caption = False]
	for i in mydict:
		if i in caption:
			post.append(mydict[i])
			# write CSV file
			line = (str(username) + ',-' + str(post_id) + ',' + str(caption) + "," + str(i) + "," + str(mydict[i]))
			if type(line) != bytes:
				line = line.encode('utf-8')

			line = str(line).replace("b'","").replace("'","") + "\n"
			line = line.replace("-",'"')
			z.write(line)

	# counting the interest/ category for every caption/convo/conversation
	counter = Counter(post)
	post = []
	total_keyword = sum(counter.values())
	
	# get total score of every interest
	nilai_interets = {}
	str_nilai_interest = ""
	for i in list_interest:
		nilai_interets[i] = count_percent(counter[i], total_keyword)

	if username not in user_post:
		user_post[username] = {}

	# get count of followers/following for every user
	for i in list_interest:
		if str(i) not in user_post[username]:
			user_post[username][i] = 0
		user_post[username][i] += nilai_interets[i]

	print ("Convo-" + str(hit) + " was checked. Username: " + username)
for i in user_list:
	str_nilai_interest_user = ""
	for j in user_post[i]:
		 str_nilai_interest_user = str_nilai_interest_user + "," + str(user_post[i][j])

	#total_percent = user_post[i]['traveling'] + user_post[i]['fashion'] + user_post[i]['culinary'] + user_post[i]['music']
	total_percent = sum(user_post[i].values())
	str_percent_total = ""
	percent_total = {}
	for j in list_interest:
		percent_total[j] = count_percent(user_post[i][j], total_percent)
		str_percent_total = str_percent_total + "," + str(percent_total[j])

	#Header = "Username,Sum of interest"
	g.write(str(i) + str_percent_total + "\n")
g.close()
z.close()
print("Done!")