# /bin/python
import csv
import json
import unicodedata  
import MySQLdb
import pymysql
import datetime

json_users 			= 'yelp_academic_dataset_user.json'
json_busnss 		= 'yelp_academic_dataset_business.json'
json_reviews 		= 'yelp_academic_dataset_review.json'


csv_ratings_users_busnss 				= 'ratings_users_busnss.csv'
csv_ratings_users_rests 				= 'ratings_users_rests.csv'
csv_ratings_elite_users_busnss 			= 'ratings_elite_users_busnss.csv'
csv_ratings_elite_users_rests 			= 'ratings_elite_users_rests.csv'

csv_user_key_errors 			= 'user_key_errors.csv'
csv_busnss_key_errors			= 'busnss_key_errors.csv'

path_json_files			= '/Users/paridhi/thesis/json_data/'
path_csv_files			= '/Users/paridhi/thesis/csv_data/'


dictUsers 			= {}
dictEliteUsers 		= {}
dictBusnss 			= {}
dictRests 			= {}
dictRatingsBusnss 	= {}
dictRatingsRests 	= {}


#db = MySQLdb.connect(host="localhost",    
#                     user="root",         
#                     passwd="Pass1234", 
#                     db="yelp_all") 
 
# pip install pymysql                    
db = pymysql.connect(host="localhost",
                     user="root",
                     passwd="Pass1234",
                     db="yelp_all")
                                          
def populate_datasets_db():

	cur = db.cursor()
	
	cur.execute("SELECT * FROM users")
	for row in cur.fetchall():
		userName = row[0]
		userId = row[1]
		dictUsers[userId]=userName
		
	cur.execute("SELECT * FROM elite_users")
	for row in cur.fetchall():
		userName = row[0]
		userId = row[1]
		dictEliteUsers[userId]=userName
	
	cur.execute("SELECT * FROM busnss")
	for row in cur.fetchall():
		busnssName = row[0]
		busnssId = row[1]
		dictBusnss[busnssId]=busnssName	
		
	cur.execute("SELECT * FROM rests")
	for row in cur.fetchall():
		busnssName = row[0]
		busnssId = row[1]
		dictRests[busnssId]=busnssName
		
	db.close()
	
def create_users_ratings_dataset():

	
	# Ratings for normal users for all businesses
	fRatingsUsersBusnss 			= open(path_csv_files+csv_ratings_users_busnss, 'wb')
	wRatingUsersBusnss 				= csv.writer(fRatingsUsersBusnss)
	
	# Ratings for normal users for restaurant businesses
	fRatingsUsersRests	 			= open(path_csv_files+csv_ratings_users_rests, 'wb')
	wRatingUsersRests 				= csv.writer(fRatingsUsersRests)
	
	# Ratings for elite users for all businesses
	fRatingsEliteUsersBusnss 			= open(path_csv_files+csv_ratings_elite_users_busnss, 'wb')
	wRatingEliteUsersBusnss 			= csv.writer(fRatingsEliteUsersBusnss)
	
	# Ratings for elite users for restaurant businesses
	fRatingsEliteUsersRests	 			= open(path_csv_files+csv_ratings_elite_users_rests, 'wb')
	wRatingEliteUsersRests 				= csv.writer(fRatingsEliteUsersRests)
	
	# Incase ratings donot have a valid userid or businessid in users or business datasets, log the ids in error logs
	fUserKeyErrs	 	= open(path_csv_files+csv_user_key_errors, 'wb')
	wUserKeyErrs 		= csv.writer(fUserKeyErrs)
	fBusnssKeyErrs	 	= open(path_csv_files+csv_busnss_key_errors, 'wb')
	wBusnssKeyErrs 		= csv.writer(fBusnssKeyErrs)
	
	
	wRatingUsersBusnss.writerow(["user_name","user_id","business_name","business_id","rating"])
	wRatingUsersRests.writerow(["user_name","user_id","business_name","business_id","rating"])
	wRatingEliteUsersBusnss.writerow(["user_name","user_id","business_name","business_id","rating"])
	wRatingEliteUsersRests.writerow(["user_name","user_id","business_name","business_id","rating"])
	
	with open(path_json_files+json_reviews) as rFile:
	
		for rating in rFile:
			row = json.loads(rating)
			
			rating	= row['stars']
			busnssId = str(row['business_id'])
			
			# if there is a key error and the businessId does not exist in the business dataset			
			if busnssId not in dictBusnss.keys():
				wBusnssKeyErrs.writerow([busnssId])
				continue
			busnssName = dictBusnss[busnssId]
						
			userId 	= str(row['user_id'])	
			
			# Case 1 : This user is a normal user		
			if userId in dictUsers.keys():
			
				userName 		= dictUsers[userId]
				wRatingBusnss 	= wRatingUsersBusnss
				wRatingRests 	= wRatingUsersRests
				
			# Case 2 : This user is an elite user
			elif userId in dictEliteUsers.keys():
			
				userName 		= dictEliteUsers[userId]
				wRatingBusnss 	= wRatingEliteUsersBusnss
				wRatingRests 	= wRatingEliteUsersRests
				
			# Case 3 : There is a key error and the userId does not exist in the users and eliteUsers dataset	
			else:
			  	wUserKeyErrs.writerow([userId])
				continue	
			
			wRatingBusnss.writerow([userName,userId,busnssName,busnssId,rating])
			if busnssId in dictRests.keys():
				wRatingRests.writerow([userName,userId,busnssName,busnssId,rating])
  		
	fRatingsUsersBusnss.close()
	fRatingsUsersRests.close()
	fRatingsEliteUsersBusnss.close()
	fRatingsElitesersRests.close()
	fUserKeyErrs.close()
	fBusnssKeyErrs.close()

populate_datasets_db()	 
create_users_ratings_dataset()

