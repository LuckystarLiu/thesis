# /bin/python

# The user dataset need to be split in the ratio provided (default = 70 percent)
# Once user dataset is split, its ratings can be split accordingly, using business dataset
# business dataset on its own need not be split
# Based on the original ratio of users to elite users, the elite user dataset will be split randomly too keeping 
# the same ratio

import random
import csv
import pymysql
import unicodedata
from math import sqrt
import MySQLdb
import pandas

#debug mode ON/OFF
debug=0

path 				= '/Users/paridhi/thesis/csv_data/' 
#path 				= '/home/ec2-user/yelp/csv_data/'
path_train			= 'train_'
path_test			= 'test_'

users_file 				= 'users.csv'
elite_users_file 		= 'elite_users.csv'

users_ratings_busnss_file 			= 'users_ratings_busnss.csv'
users_ratings_rests_file 			= 'users_ratings_rests.csv'
elite_users_ratings_busnss_file 	= 'elite_users_ratings_busnss.csv'
elite_users_ratings_rests_file 		= 'elite_users_ratings_rests.csv'

path 				= '/Users/paridhi/thesis/code2/csv_data/'

path_train			= '/Users/paridhi/thesis/code2/csv_data/'
path_test			= '/Users/paridhi/thesis/code2/csv_data/'
user_ratings_train_file		= 'train_user_ratings.csv'
user_ratings_test_file		= 'test_user_ratings.csv'

# waiting for program to complete on 31st, but moving on
#user_ratings_train_file		= 'user_ratings_train.csv'
#user_ratings_test_file		= 'user_ratings_test.csv'
#path_train			= '/Users/paridhi/thesis/code2/csv_data/train/'
#path_test			= '/Users/paridhi/thesis/code2/csv_data/test/'

dictUsers 			= {}
dictEliteUsers 		= {}
dictBusnss 			= {}
dictRests 			= {}

dictRatingsRestsUsers 	= {}
dictRatingsRestsEliteUsers 	= {}


random.seed(350)


#db = MySQLdb.connect(host="localhost",    
#                     user="root",         
#                     passwd="Pass1234", 
#                     db="yelp_all") 
 
# pip install pymysql                    
db = pymysql.connect(host="localhost",
                     user="root",
                     passwd="Pass1234",
                     db="yelp_all")
                                          
def populateDictsFromDB():

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
		
	cur.execute("SELECT * FROM ratings_elite_users_rests")
	for row in cur.fetchall():
		busnssName = row[0]
		busnssId = row[1]
		dictRatingsRestsEliteUsers[busnssId]=busnssName
	
	cur.execute("SELECT * FROM ratings_users_rests")
	for row in cur.fetchall():
		userName 	= row[0]
		userId		= row[1]
		itemName	= row[2]
		itemId		= row[3]
		rating 		= row[4]
		# Make a unique key since ratings dataset has rows uniquely identified by a user rating an item
		key=userId+","+itemId
		dictRatingsRestsUsers[key]=[userId,userName,itemId,itemName,rating]
			
	db.close()

def splitTrainTestUsers(percent=70):
	
	# Calculate the number of training and test users based on percent provided
	totalUsers 			= len(dictUsers)
	noTrainUsers		= (percent*totalUsers)/100
	noTestUsers			= totalUsers - noTrainUsers
	
	# Create list of userids for test data
	testUsers = random.sample(dictUsers, noTestUsers)
	print "no of test users are = ", noTestUsers

	# Training set files
	fUsersTrain			= open(path_train + users_file, 'wb')
	wUsersTrain			= csv.writer(fUsersTrain)

	# Test set files
	fUsersTest			= open(path_test + users_file, 'wb')
	wUsersTest			= csv.writer(fUsersTest)
	

	wUsersTrain.writerow(["name","user_id","average_stars","review_count","yelping_since"])
	wUsersTest.writerow(["name","user_id","average_stars","review_count","yelping_since"])
	
	with open(path + users_file) as file:
		reader = csv.DictReader(file)
		for row in reader:

			if str(row['user_id']) in testUsers:
				wUsers = wUsersTest
			else:
				wUsers = wUsersTrain
	
			wUsers.writerow([row['name'],row['user_id'],row['average_stars'],row['review_count'],row['yelping_since'],row['elite']])
	
	fUsersTrain.close()
	fUsersTest.close()

def splitTrainTestEliteUsers(percent=70):

	# Calculate the number of training and test users based on percent provided
	totalEliteUsers 		= len(dictEliteUsers)
	noTrainEliteUsers		= (percent*totalEliteUsers)/100
	noTestEliteUsers		= totalEliteUsers - noTrainEliteUsers
	
	# Create list of userids for test data
	testEliteUsers = random.sample(dictEliteUsers, noTestEliteUsers)
	print "no of test elite users are = ", noTestEliteUsers

	# Training set files
	fEliteUsersTrain		= open(path_train + elite_users_file, 'wb')
	wEliteUsersTrain		= csv.writer(fEliteUsersTrain)

	# Test set files
	fEliteUsersTest			= open(path_test + elite_users_file, 'wb')
	wEliteUsersTest			= csv.writer(fEliteUsersTest)
	

	wEliteUsersTrain.writerow(["name","user_id","average_stars","review_count","yelping_since"])
	wEliteUsersTest.writerow(["name","user_id","average_stars","review_count","yelping_since"])
	
	with open(path + elite_users_file) as file:
		reader = csv.DictReader(file)
		for row in reader:

			if str(row['user_id']) in testEliteUsers:
				wUsers = wEliteUsersTest
			else:
				wUsers = wEliteUsersTrain
	
			wUsers.writerow([row['name'],row['user_id'],row['average_stars'],row['review_count'],row['yelping_since'],row['elite']])
	
	fEliteUsersTrain.close()
	fEliteUsersTest.close()

def splitTrainTestEliteUsers(percent=70):

	# Calculate the number of training and test users based on percent provided
	totalEliteUsers 		= len(dictEliteUsers)
	noTrainEliteUsers		= (percent*totalEliteUsers)/100
	noTestEliteUsers		= totalEliteUsers - noTrainEliteUsers
	
	# Create list of userids for test data
	testEliteUsers = random.sample(dictEliteUsers, noTestEliteUsers)
	print "no of test elite users are = ", noTestEliteUsers

	# Training set files
	fEliteUsersTrain		= open(path_train + elite_users_file, 'wb')
	wEliteUsersTrain		= csv.writer(fEliteUsersTrain)

	# Test set files
	fEliteUsersTest			= open(path_test + elite_users_file, 'wb')
	wEliteUsersTest			= csv.writer(fEliteUsersTest)
	

	wEliteUsersTrain.writerow(["name","user_id","average_stars","review_count","yelping_since"])
	wEliteUsersTest.writerow(["name","user_id","average_stars","review_count","yelping_since"])
	
	with open(path + elite_users_file) as file:
		reader = csv.DictReader(file)
		for row in reader:

			if str(row['user_id']) in testEliteUsers:
				wUsers = wEliteUsersTest
			else:
				wUsers = wEliteUsersTrain
	
			wUsers.writerow([row['name'],row['user_id'],row['average_stars'],row['review_count'],row['yelping_since'],row['elite']])
	
	fEliteUsersTrain.close()
	fEliteUsersTest.close()

# Directly fetches ratings data from the db		
def getUserRatingsFromDB(userId,elite=0,test=0):
	userRatings = {}
	userName=""
	try:
		cur = db.cursor()
		if elite:
			table=eliteUsersRatingsTable
		else:
			if test:
				table=trainUsersRatingsTable
			else:
				table=testUsersRatingsTable
		cur.execute("SELECT * FROM "+table)
		print "total rows fetched from ", table, " is ",cur.rowcount
		for row in cur.fetchall():
			if userId == row[1]:
				itemId 		= row[3]
				rating  	= row[4]		
				userRatings[itemId] = rating
		return userRatings
	except connector.Error,e:
		print(e)
	finally:
		cur.close()

def getAllUsersRatingsFromDB():
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+trainUsersRatingsTable)
		print "total rows fetched from ", trainUsersRatingsTable, " is ",cur.rowcount
		allRatings = {}
		for row in cur.fetchall():
			userId 		= row[1]
			itemId 		= row[3]
			rating  	= row[4]
			if userId not in allRatings.keys():
				allRatings[userId]={}	
			allRatings[userId][itemId] = [rating]
		return cur.rowcount, allRatings
	except connector.Error,e:
		print(e)
	finally:
		cur.close()

# This fn ran only once, created 2 csv files with a split of 70:30 for user rest ratings only.
# created train ratings=1005886 , test ratings=174205, total user rest ratings = 1180091 in db, matches,split is 14%


def splitTrainTestUserRatings(trainPercent=70):
	
	# Calculate the number of users ratings based on percent provided
	totalRatings 		= len(dictRatingsRestsUsers)
	noTrainRatings		= (trainPercent*totalRatings)/100
	noTestRatings		= totalRatings - noTrainRatings
	
	# Create list of userids and itemids for test data
	sampledTestRatings = random.sample(dictRatingsRestsUsers, noTestRatings)
	if debug ==1:
		print "no of test ratings is = ", noTestRatings, "no of train ratings is = ", noTrainRatings
		
	# Training set files
	fRatingsTrain		= open(path_train + user_ratings_train_file, 'wb')
	wRatingsTrain		= csv.writer(fRatingsTrain)

	# Test set files
	fRatingsTest		= open(path_test + user_ratings_test_file, 'wb')
	wRatingsTest		= csv.writer(fRatingsTest)
	

	wRatingsTrain.writerow(["user_name","user_id","item_name","item_id","rating"])
	wRatingsTest.writerow(["user_name","user_id","item_name","item_id","rating"])

	for key,value in dictRatingsRestsUsers.items():
		if key in sampledTestRatings:
			wRatingsTest.writerow(value)
		else:	
			wRatingsTrain.writerow(value)

def multiFoldCrossValUserRatings(noFolds=5):
	
	# Calculate the number of users ratings based on percent provided
	totalRatings 		= len(dictRatingsRestsUsers)
	noTrainRatings		= (trainPercent*totalRatings)/100
	noTestRatings		= totalRatings - noTrainRatings
	
	# Create list of userids and itemids for test data
	sampledTestRatings = random.sample(dictRatingsRestsUsers, noTestRatings)
	if debug ==1:
		print "no of test ratings is = ", noTestRatings, "no of train ratings is = ", noTrainRatings
		
	# Training set files
	fRatingsTrain		= open(path_train + user_ratings_train_file, 'wb')
	wRatingsTrain		= csv.writer(fRatingsTrain)

	# Test set files
	fRatingsTest		= open(path_test + user_ratings_test_file, 'wb')
	wRatingsTest		= csv.writer(fRatingsTest)
	

	wRatingsTrain.writerow(["user_name","user_id","item_name","item_id","rating"])
	wRatingsTest.writerow(["user_name","user_id","item_name","item_id","rating"])

	for key,value in dictRatingsRestsUsers.items():
		if key in sampledTestRatings:
			wRatingsTest.writerow(value)
		else:	
			wRatingsTrain.writerow(value)
					
populateDictsFromDB()
df = pandas.DataFrame(np.random.randn(100, 4), columns=list('ABCD'))

#splitTrainTestUserRatings()
#splitTrainTestUsers()
#splitTrainTestEliteUsers()