from collections import Counter
import pandas as pd

#------------------ CONFIGURATION -------------------------------
input_filename = "convo.csv"

#load convo
convo = pd.read_csv(input_filename)

#nama baris di "input_filename"
head_userid = 'user_id'
head_username = 'username'
head_caption = 'caption'
head_postid = 'post_id'
head_likes_count = 'like_count'
head_comment_count = 'comment_count'
head_brand = 'brand'
head_date_post = 'date_post'
head_follower_count = 'follower_count'
head_following_count = 'following_count'
#------------------ DATA LOADING --------------------------------
print ("Memulai")
#Pencatatan. Hasilnya nantinya akan | count_user_posts['username'] = jumlah post |
count_user_posts = {}
count_user_likes = {}
count_user_comments = {}
count_user_followers = {}
count_user_following = {}

tb_statistic_user = []
tb_statistic_convo = []
tb_username = []
count_user_post = {}
for index, row in convo.iterrows():
	user_id = row[head_userid].replace('"', '').replace("'","")
	username = row[head_username].replace('"',"").replace(",","|").replace("'","")
	caption = str(row[head_caption]).replace(",","").replace(r"\u","|").replace("\r","").replace("\n","").replace("'","").lower().strip()
	post_id = row[head_postid].replace('"','').replace("'","")
	brand = row[head_brand]
	timestamp = row['timestamp']
	count_likes = str(row[head_likes_count])
	count_comments = str(row[head_comment_count])
	engagement_value = str(row[head_likes_count] + row[head_comment_count])


	follower_count = row[head_follower_count]
	following_count = row[head_following_count] 

	if username not in tb_username:
		tb_username.append(username)

	tb_statistic_convo.append({
		'brand' : brand,
		'username' : username,
		'post_id' : '"' + str(post_id),
		'caption' : '"' + str(caption),
		'timestamp' : timestamp,
		'like_count' : count_likes,
		'comment_count' : count_comments,
		'engagement' : engagement_value
		})
	#================= Awal perhitungan jumlah post, like, komen, following, followers ==============================
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
	count_user_followers[username] = row[head_follower_count]
	count_user_following[username] = row[head_following_count] 
	#================= Akhir perhitungan jumlah post, like, komen, following, followers ==============================

df_statstic_convo = pd.DataFrame.from_dict(tb_statistic_convo)
columns_order = ['brand', 'username', 'post_id', 'caption', 'timestamp', 'like_count', 'comment_count', 'engagement']
df_statstic_convo[columns_order].to_csv('performance_convo.csv')
print("performance_convo.csv")
for username in tb_username:
	total_post = count_user_posts[username]
	total_like = count_user_likes[username]
	average_like = total_like / total_post
	total_comment = count_user_comments[username]
	average_comment = total_comment / total_post
	engagement = average_like + average_comment
	follower_count = count_user_followers[username]
	following_count = count_user_following[username]
	reach = follower_count * 0.3

	tb_statistic_user.append({
		'username': username,
		'total_post': total_post,
		'total_like': total_like,
		'average_like': average_like,
		'total_comment': total_comment,
		'average_comment': average_comment,
		'engagement': engagement,
		'follower_count': follower_count,
		'following_count': following_count,
		'reach': reach
		})
df_statstic_user = pd.DataFrame.from_dict(tb_statistic_user)
columns_user_order =['username','total_post','total_like','average_like','total_comment','average_comment','engagement','follower_count','following_count','reach']
df_statstic_user[columns_user_order].to_csv('performance_user.csv')
print("performance_user.csv")