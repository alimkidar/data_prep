import pandas as pd
import requests, json, re, datetime
from logger import Logger
time_now = str(int(datetime.datetime.now().timestamp()))


#------------------ CONFIGURATION -------------------------------
input_filename = "output/convo.csv"
print_every = 10
print_every_bak = print_every
head_username = 'username'

print ("Initializing...")
#load convo
df_convo = pd.read_csv(input_filename)
today_date = datetime.datetime.today()
print(today_date.strftime('%A, %d-%B-%Y'))

def save_df(dict_,output_,df_old):
	df_profile = pd.DataFrame.from_dict(dict_)
	
	#Merge input data with new data
	for fieldname in list(df_profile):
		df_old[fieldname] = df_profile[fieldname]
	#column_order = ['username','full_name','biography','external_url','follower_count','following_count','has_channel','user_id','is_business_account','business_category_name','business_email','business_phone_number','is_private','is_verified','profile_pic_url','connected_fb_page','media_count', 'status']
	if 'index' in list(df_old):
		df_old = df_old.drop(columns=['index'])
	df_old.reset_index()
	df_old.index.names = ['index']
	df_old.to_csv(output_,index=True)
	return print("saved as " + output_)

#Untuk ambil jumlah followers dan following di instagram
def insta(user_name):
	headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
	ff = []
	situs = 'https://www.instagram.com/' + user_name + '/'
	status = 'InternetError'
	# OK = sukses, NotFound = sukses (username tidak ditemukan), InternetError = error(internet bermasalah)
	try: 
		response = requests.get(url=situs, headers=headers)
		if response.status_code == requests.codes.ok:
			status = 'OK'
		else:
			status = 'UsernameNotFound'
	except: 
		print ("Koneksi Internet Error!" + " Username: " + user_name + " Gagal diproses!")
		status = 'InternetError'
	try:
		json_match = re.search(r'window\._sharedData = (.*);</script>', response.text)
		profile_json = json.loads(json_match.group(1))['entry_data']['ProfilePage'][0]['graphql']['user']
		biography = profile_json['biography']
		external_url = profile_json['external_url']
		follower_count = profile_json['edge_followed_by']['count']
		following_count = profile_json['edge_follow']['count']
		full_name = profile_json['full_name']
		has_channel = profile_json['has_channel']
		user_id = profile_json['id']
		is_business_account = profile_json['is_business_account']
		business_category_name = profile_json['business_category_name']
		business_email = profile_json['business_email']
		business_phone_number = profile_json['business_phone_number']
		is_private = profile_json['is_private']
		is_verified = profile_json['is_verified']
		profile_pic_url = profile_json['profile_pic_url']
		username = profile_json['username']
		connected_fb_page = profile_json['connected_fb_page']
		media_count = profile_json['edge_owner_to_timeline_media']['count']
	except:
		biography = '-' 
		external_url = '-' 
		follower_count = '-' 
		following_count = '-' 
		full_name = '-' 
		has_channel = '-' 
		user_id = '-' 
		is_business_account = '-' 
		business_category_name = '-' 
		business_email = '-' 
		business_phone_number = '-' 
		is_private = '-' 
		is_verified = '-' 
		profile_pic_url = '-' 
		username = user_name
		connected_fb_page = '-' 
		media_count = '-' 
	profile = ({
		'username': username,
		'full_name': full_name,
		'biography': biography,
		'external_url': external_url,
		'follower_count': follower_count,
		'following_count': following_count,
		'has_channel': has_channel,
		'user_id': user_id,
		'is_business_account': is_business_account,
		'business_category_name': business_category_name,
		'business_email': business_email,
		'business_phone_number': business_phone_number,
		'is_private': is_private,
		'is_verified': is_verified,
		'profile_pic_url': profile_pic_url,
		'connected_fb_page': connected_fb_page,
		'media_count': media_count,
		'status': status
		})
	return profile
#------------------ DATA LOADING --------------------------------
print ("Memulai")
dict_profile = []
hit = 1

for index, row in df_convo.iterrows():
	username = str(row[head_username])
	if username == 'nan':
		username = ''
	profile = insta(username)
	user_id = profile['user_id']
	dict_profile.append(profile)

	if user_id == '-':
		print(str(hit) + '. ' + username + ' Gagal')
	else:
		print(str(hit) + '. ' + username + ' Berhasil')
	hit += 1
	print_every -= 1

	if print_every <= 0:
		save_df(dict_profile,'convo_enriched.csv',df_convo)
		print_every = print_every_bak

save_df(dict_profile,'output/convo.csv',df_convo)
save_df(dict_profile,'temp/convo_enriched_' + time_now + '.csv',df_convo)
Logger('[' + time_now + " file {'convo.csv'} had been enriched with name " + "'temp/convo_enriched_" + time_now + ".csv'\n").write_log()
print("Done")