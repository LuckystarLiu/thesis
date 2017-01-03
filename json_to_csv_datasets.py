# /bin/python
import csv
import json
import unicodedata  
              
file_json_users 		= 'yelp_academic_dataset_user.json'
file_json_busnss 		= 'yelp_academic_dataset_business.json'
file_json_reviews 		= 'yelp_academic_dataset_review.json'

file_csv_users 			= 'users.csv'
file_csv_elite_users 	= 'elite_users.csv'
file_csv_busnss 		= 'busnss.csv'
file_csv_rests 			= 'rests.csv'
file_csv_ratings 		= 'ratings.csv'

path_json_files			= '/Users/paridhi/thesis/json_data/'
path_csv_files			= '/Users/paridhi/thesis/csv_data/'

# via http://stackoverflow.com/a/518232
def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
 
def convert_split_user():

	fUser 		= open(path_csv_files+file_csv_users, 'wb')
	fEliteUser 	= open(path_csv_files+file_csv_elite_users, 'wb')  
	
	wUser 		= csv.writer(fUser)
	wEliteUser 	= csv.writer(fEliteUser)
	
	wUser.writerow(["name","user_id","average_stars","review_count","yelping_since","elite"])
	wEliteUser.writerow(["name","user_id","average_stars","review_count","yelping_since","elite"])
	
	with open(path_json_files+file_json_users) as jFile:
	
		for jObject in jFile:
			row = json.loads(jObject)
			
			if not row['elite']:
				wUser.writerow([row['name'].encode('utf-8'),row['user_id'],row['average_stars'],row['review_count'],row['yelping_since'],row['elite']])
			else:
				wEliteUser.writerow([row['name'].encode('utf-8'),row['user_id'],row['average_stars'],row['review_count'],row['yelping_since'],row['elite']])
	
	fUser.close()
	fEliteUser.close()
	 
                 
def convert_busnss():

	fBusnss 		= open(path_csv_files+file_csv_busnss, 'wb') 
	wBusnss 		= csv.writer(fBusnss)
	wBusnss.writerow(["name","business_id","city","state","categories","stars", "review_count"])
	
	with open(path_json_files+file_json_busnss) as jFile:
	
		for jObject in jFile:
			row = json.loads(jObject)
			
			wBusnss.writerow([row['name'].encode('utf-8'), row['business_id'], strip_accents(row['city']), row['state'], '|'.join(row['categories']), row['stars'], row['review_count']])
			
	fBusnss.close()
	
def filter_rests():

	fRests 		= open(path_csv_files+file_csv_rests, 'wb') 
	wRests 		= csv.writer(fRests)
	wRests.writerow(["name","business_id","city","state","categories","stars", "review_count"])  
		
	with open(path_json_files+file_json_busnss) as jFile:
	
		for jObject in jFile:
			row = json.loads(jObject)
			
			#if strip_accents(data['categories']) == 'Edinburgh':
			if ("Cafes" in row['categories'] or "Restaurants" in row['categories'] or "Salads" in row['categories'] or "Food" in row['categories'] or "Bars" in row['categories'] or "Nightlife" in row['categories'] or "Coffee & Tea" in row['categories'] or "Breakfast & Brunch" in row['categories'] or "Diners" in row['categories'] or "Steakhouses" in row['categories'] or "Ice Cream & Frozen Yogurt" in row['categories'] or "Fast Food" in row['categories'] or "Wine Bars" in row['categories']):
		  	#              print (data['categories']) 
		  		cat = '|'.join(row['categories'])
		  		wRests.writerow([row['name'].encode('utf-8'), row['business_id'], strip_accents(row['city']), row['state'], '|'.join(row['categories']), row['stars'], row['review_count']])
	fRests.close()

# convert_split_user()
# after splitting ratio of users to elite user is ~ 16.5 : 1
# no of non-elite users - 520879
# no of elite users - 31462
# convert_busnss()	
# cat csv_data/busnss.csv | wc -l
#   77446
# filter_rests()
# cat csv_data/rests.csv | wc -l
#   34655
