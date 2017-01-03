# /bin/python

from __future__ import division
#import random
import csv
import pymysql
#import unicodedata
from math import sqrt
import datetime
import pickle
#import time
#import numpy as np

path 				= '/Users/paridhi/thesis/code2/csv_data/' 
#path 				= '/home/ec2-user/code2/csv_data/'
trainUsersDBTableName = "train_ratings_train_"
valUsersDBTableName  	= "train_ratings_val_"
#trainUsersDBTableName 	= "small_train_ratings_train_"
#valUsersDBTableName  	= "small_train_ratings_val_"
#expertsRatingsDBTable = "expert_ratings_500_"
#expertsRatingsDBTable = "expert_ratings_all_"

path_train			= 'train_'
path_test			= 'test_'
sim					= 'sim-loadtest/'
debug =0

test_users_list 	= 'test_users_list.csv'
results_file		= "results_"
resultsMAESimilarity_file		= "/results/resultsMAESimilarity_"
resultsMAECoverage_file			= "/results/resultsMAESimilarity_"
dictUsers 			= {}
dictEliteUsers 		= {}
dictItems 			= {}
dictUserRatings 	= {}
dictEliteRatings 	= {}

usersTable	="users"

testUsersListTable = "test_users_list"
itemsTable = "rests"
item = "restaurant"
testResultsTable="results_exp1"
trainUsersRatingsTable	= "train_ratings" #"train_ratings"
testUsersRatingsTable   ="test_ratings"

# Expert details table in DB
expertsDetailsDBTable = "elite_users"#_min_rating_500" #"elite_users"
trainUsersRatingsDict ={}
testUsersRatingsDict ={}
eliteRatingsDict ={}

dbName 						= "yelp_all"

connector 					= pymysql # MySQLdb
                  
db = connector.connect(host="localhost",
                     user="root",
                     passwd="Pass1234",
                     db=dbName)
                     
thresholdLambda = 0.01 # minimum similarity score to pick a neighbour
#thresholdAlpha  = 

def saveAllUsersRatingsToPickle(ratings,elite=0,test=0):
	if elite:
		file=allEliteUsersRatingsFile
	else:
            if test:
                file=testAllUsersRatingsFile
            else:
		file=trainAllUsersRatingsFile
	with open(path + file, 'wb') as handle:
		pickle.dump(ratings, handle, protocol=pickle.HIGHEST_PROTOCOL)
def saveAllUsersRatingsTopN():
	testSet={}
    	trainSet={}
    	ratingsSet = trainUsersRatingsDict
    	noRows = len(ratingsSet)
    	sampleSize = (3*noRows)/10
    	testUserIds = random.sample(list(ratingsSet.keys()), sampleSize)
    	print "start split, sample size is",sampleSize
    	for userId,userDict in ratingsSet.items():
    		if userId in testUserIds:
    			testSet[userId]=userDict
    		else:
    			trainSet[userId]=userDict
    	#print "start dump for train"
	with open(path + trainSetTopNFile, 'wb') as handle:
		pickle.dump(trainSet, handle, protocol=pickle.HIGHEST_PROTOCOL)
	#print "start dump for test"
	with open(path + testSetTopNFile, 'wb') as handle:
		pickle.dump(testSet, handle, protocol=pickle.HIGHEST_PROTOCOL)
def getAllUsersRatingsTopN():

    	with open(path + trainSetTopNFile , 'rb') as handle:
   		trainSet = pickle.load(handle)
   	with open(path + testSetTopNFile , 'rb') as handle:
   		testSet = pickle.load(handle)	
   	   	return trainSet,testSet   			
def getAllUsersRatingsFromPickle(elite=0,test=0):
	if elite:
		file=allEliteUsersRatingsFile
	else:
            if test:
                file=testAllUsersRatingsFile
            else:
                file=trainAllUsersRatingsFile
        with open(path + file , 'rb') as handle:
   		ratings = pickle.load(handle)
   	return ratings
def getUserName(userId,elite=0):
	try:
		cur = db.cursor()
		if elite:
			table=expertDBTable
		else:
			table=usersTable
		cur.execute("SELECT * FROM "+table+ " WHERE USER_ID = %s ",(userId))#('PP_xoMSYlGr2pb67BbqBdA'))
		row = cur.fetchone()
		if row:
			return row[0]		
		else:
			return ""
	except connector.Error,e:
		print(e)
	finally:
		cur.close()
def populateExpertsDict():
	try:
		expertsDict	={}
		cur = db.cursor()
		cur.execute("SELECT * FROM "+ expertsDetailsDBTable)
		for row in cur.fetchall():
			expertName	=	row[0]
			expertId	=	row[1].replace('\"','')
			avgStars	=	row[2]
			reviewCount	=	row[3]
			 
			expertsDict[expertId]	=	[expertName,expertId,avgStars,reviewCount]	
		if debug:
			print "Experts dict populated, len is",	len(expertsDict)
		return expertsDict
	except connector.Error,e:
		print(e)
	finally:
		cur.close()			
def getMinRatingExpert(expertsDict,expertId):

	expertId = expertId.replace('\"','')
	if expertId not in expertsDict.keys():
		print "Error : Populate expert dict first!"
		return
	[expertName,expertId,avgStars,reviewCount]	= expertsDict[expertId]
	return reviewCount				
# This fn remains same for training or test set, since all business data is in yelp_all.rests table
# So there is no variable test.
def getItemName(itemId):
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+ itemsTable+ " WHERE BUSINESS_ID = %s ",(itemId))
		row = cur.fetchone()
		if row:
			return row[0]		
		else:
			return ""
	except connector.Error,e:
		print(e)
	finally:
		cur.close()
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
				itemId 		= row[3].replace('\"','')
				rating  	= row[4]		
				userRatings[itemId] = rating
		return userRatings
	except connector.Error,e:
		print(e)
	finally:
		cur.close()		
def getAllItemsFromDB():
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+itemsTable)
		itemsList = []
		for row in cur.fetchall():
			itemsList.append(row[1].replace('\"',''))
		return  itemsList
	except connector.Error,e:
		print(e)
	finally:
		cur.close()
def getAllRatingsTopN(noRatings=10000):
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+trainUsersRatingsTable)
		print "total rows fetched from ", trainUsersRatingsTable, " is ",cur.rowcount
		allRatings = {}
		for row in cur.fetchall():
			userId 		= row[1].replace('\"','')
			itemId 		= row[3].replace('\"','')
			rating  	= row[4]
			if userId not in allRatings.keys():
				allRatings[userId]={}	
			allRatings[userId][itemId] = [rating]
		return cur.rowcount, allRatings
	except connector.Error,e:
		print(e)
	finally:
		cur.close()		
def getUserRatingsFromDict(userId,trainSet):
	if userId not in trainSet.keys():
		# new user, no history in training set
		return {}
	userRatings = {}
	for key,value in trainSet[userId].items():
		userRatings[key] = value[0]
	return userRatings	
def getExpertRatingsFromDict(expertId,expertSet):
	if expertId not in expertSet.keys():
		# new expert, highly unlikely , no history in expert ratings set
		return {}
	expertRatings = {}
	for key,value in expertSet[expertId].items():
		expertRatings[key] = value[0]
	return expertRatings
def getAvgItemRatingFromUsers(itemId,foldNo):
	cur = db.cursor()	
	itemId=itemId.replace('\"','')		
	cur.execute("SELECT RATING FROM "+ trainUsersDBTableName+str(foldNo) + " WHERE ITEM_ID = %s ",(itemId))	
	numRatings	=0.0
	sumRatings	=0.0
	avgRating	=0.0
	for row in cur.fetchall():
		numRatings = numRatings	+1
		sumRatings = sumRatings	+row[0]
	if numRatings:
		avgRating = sumRatings/numRatings
	return avgRating
def getAvgItemRatingFromExperts(itemId,foldNo,expertsRatingsDBTable):
	cur = db.cursor()
	itemId=itemId.replace('\"','')	
	#cur.execute("SELECT RATING FROM "+ expertsRatingsDBTable + str(foldNo) + " WHERE ITEM_ID = %s ",(itemId))
	cur.execute("SELECT RATING FROM "+ expertsRatingsDBTable+ " WHERE ITEM_ID = %s ",(itemId))
	numRatings	=0.0
	sumRatings	=0.0
	avgRating	=0.0
	for row in cur.fetchall():
		numRatings = numRatings	+1
		sumRatings = sumRatings	+row[0]
	if numRatings:
		avgRating = sumRatings/numRatings
	return avgRating
	
def getTrainRatingsFromDB(dbTable):
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+dbTable)
		if debug ==1:
			print "total rows fetched from ", dbTable, " is ",cur.rowcount
		allRatings = {}
		for row in cur.fetchall():
			userId 		= row[1].replace('\"','')
			itemId 		= row[3].replace('\"','')
			rating  	= row[4]
			if userId not in allRatings.keys():
				allRatings[userId]={}	
			allRatings[userId][itemId] = [rating]
		return allRatings
	except connector.Error,e:
		print(e)
	finally:
		cur.close()

def getTestRatingsFromDB(dbTable):
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+dbTable)
		if debug ==1:
			print "total rows fetched from ", dbTable, " is ",cur.rowcount
		allRatings = {}
		for row in cur.fetchall():
			userName	= row[0]
			userId 		= row[1].replace('\"','')
			itemName	= row[2]
			itemId 		= row[3].replace('\"','')
			rating  	= row[4]	
			allRatings[userId+","+itemId] = [userName,userId,itemName,itemId,rating]
		return allRatings
	except connector.Error,e:
		print(e)
	finally:
		cur.close()				
def saveAllUsersRatingsToPickle(ratings,elite=0,test=0):
	if elite:
		file=allEliteUsersRatingsFile
	else:
            if test:
                file=testAllUsersRatingsFile
            else:
		file=trainAllUsersRatingsFile
	with open(path + file, 'wb') as handle:
		pickle.dump(ratings, handle, protocol=pickle.HIGHEST_PROTOCOL)
# this fn gets all the training data for all CV folds from the db and stores locally in pickle files
def saveTrainingSetsToPickle(noFolds):
	for index in range(1,noFolds+1):
		dbTable=trainUsersDBTableName+str(index)
		trainSet=getTrainRatingsFromDB(dbTable)
		file=trainUsersDBTableName+str(index)+".pickle"
		if debug:
			print "no of ratings in train set = ",len(trainSet), "saved to file =",file
		with open(path + "pickle/" + file, 'wb') as handle:
			pickle.dump(trainSet, handle, protocol=pickle.HIGHEST_PROTOCOL)

# this fn fetches all the training sets for all CV folds stored in pickle and constructs python dictionaries from them
def getTrainingSetsFromPickle(noFolds):
	trainSetList=[]
	for index in range(1,noFolds+1):
		file=trainUsersDBTableName+str(index)+".pickle"
		with open(path + "pickle/" + file, 'rb') as handle:
			#noRatings,trainSet = pickle.load(handle)
			trainSet = pickle.load(handle)
		trainSetList.append(trainSet)
		if debug:
			print "fetched train set from file = ",file, " no of training users = ",len(trainSet)
	return trainSetList
	
# this fn gets all the validation data for all CV folds from the db and stores locally in pickle files
def saveValidationSetsToPickle(noFolds):
	for index in range(1,noFolds+1):
		dbTable=valUsersDBTableName+str(index)
		valSet=getTestRatingsFromDB(dbTable)
		file=valUsersDBTableName+str(index)+".pickle"
		if debug>1:
			print "no of ratings in val set = ",len(valSet), "saved to file = ",file
		with open(path + "pickle/" + file, 'wb') as handle:
			pickle.dump(valSet, handle, protocol=pickle.HIGHEST_PROTOCOL)
# this fn fetches all the validation sets for all CV folds stored in pickle and constructs python dictionaries from them
def getValidationSetsFromPickle(noFolds):
	valSetList=[]
	for index in range(1,noFolds+1):
		file=valUsersDBTableName+str(index)+".pickle"
		with open(path + "pickle/" + file, 'rb') as handle:
			valSet = pickle.load(handle)
		valSetList.append(valSet)
		if debug:
			print "fetched validation set from file = ",file, " len of val set = ",len(valSet), " added to list "
	return valSetList

def saveExpertsRatingsAsCSV(table): # Before splitting into CV folds, run this cmd to prepare csv file from DB
	cur = db.cursor()
	try:
			filename = path + table +".csv"
			file 			= open(filename, 'wb')
			writer 			= csv.writer(file)
			writer.writerow(["userName","userId","itemName","itemId","rating"])
			cur.execute("SELECT * FROM "+ table)
			for row in cur.fetchall():
				
				userName	= row[0].replace('\"','')
				userId 		= row[1].replace('\"','')
				itemName	= row[2].replace('\"','')
				itemId 		= row[3].replace('\"','')
				rating  	= row[4]
			 	writer.writerow([userName,userId,itemName,itemId,rating])
			 		
	except connector.Error,e:
		print(e)
	finally:
		cur.close()

def saveExpertsAsCSV():
	cur = db.cursor()
	try:
			filename = path + "experts_min_rating_250.csv"
			file 			= open(filename, 'wb')
			writer 			= csv.writer(file)
			writer.writerow(["name","user_id","average_stars","review_count","yelping_since"])
			cur.execute("SELECT * FROM elite_users_min_rating_250")
			for row in cur.fetchall():
				
				name			= row[0]
				user_id 		= row[1]
				average_stars	= row[2]
				review_count 	= row[3]
				yelping_since  	= row[4]
			 	writer.writerow([name,user_id,average_stars,review_count,yelping_since])
			 		
	except connector.Error,e:
		print(e)
	finally:
		cur.close()
  			
def getAllUsersRatingsFromPickle(elite=0,test=0):
	if elite:
		file=allEliteUsersRatingsFile
	else:
            if test:
                file=testAllUsersRatingsFile
            else:
                file=trainAllUsersRatingsFile
        with open(path + file , 'rb') as handle:
   		ratings = pickle.load(handle)
   	return ratings
   	
def saveResultsCSV():
	cur = db.cursor()
	try:
			filename = path + "results/results1.csv"
			file 			= open(filename, 'wb')
			writer 			= csv.writer(file)
			writer.writerow(["userName","userId","itemName","itemId","actualRating","predictedStandardRating","predictedEliteRating","avgExpertRating","tLambda","tTao","noUsers","noExperts"])
			cur.execute("SELECT * FROM results_exp1")
			for row in cur.fetchall():
				
				userName					=	row[0]
				userId 						= 	row[1]
				itemName					= 	row[2]
				itemId 						= 	row[3]
				actualRating			  	=	row[4]
				predictedStandardRating		=	row[5]
				predictedEliteRating		=	row[6]
				avgExpertRating				=	row[7]
				tLambda						=	row[8]
				tTao						=	row[9]
				noUsers						=	row[10]
				noExperts					=	row[11]
			 	writer.writerow([userName,userId,itemName,itemId,actualRating,predictedStandardRating,predictedEliteRating,avgExpertRating,tLambda,tTao,noUsers,noExperts])
			 		
	except connector.Error,e:
		print(e)
	finally:
		cur.close()
		
def saveExpertsRatingsFromRtoPickle(noFolds):	
	try:
		for index in range(1,noFolds+1):
			expertSet={}
			cur = db.cursor()
			cur.execute("SELECT * FROM ratings_experts_100_rests")
			for row in cur.fetchall():
				userName	= row[0]
				userId 		= row[1]
				itemName	= row[2]
				itemId 		= row[3]
				rating  	= row[4]
				if userId not in expertSet.keys():
						expertSet[userId]={}	
				expertSet[userId][itemId] = [rating]
			file="expert_ratings_100"+".pickle" # expertsRatingsDBTable+str(index)+".pickle"
			if debug:
				print "For CF fold = ",index," no of expert ratings in expertSet = ",len(expertSet), "saved to file = ",file
			with open(path + "pickle/"+ file, 'wb') as handle:
				pickle.dump(expertSet, handle, protocol=pickle.HIGHEST_PROTOCOL)	 		
	except connector.Error,e:
		print(e)
	finally:
		cur.close()
# working, checked on 4/08			
def getExpertsSetsFromPickle(noFolds,expertsRatingsDBTable):
	expertSetList=[]
	for index in range(1,noFolds+1):
		file=expertsRatingsDBTable+str(index)+".pickle"
		with open(path + "pickle/"+ file, 'rb') as handle:
			expertSet = pickle.load(handle)
		expertSetList.append(expertSet)
		if debug:
			print "fetched expert set from file = ",file, " len of expertSet = ",len(expertSet), " added to list "
	return expertSetList
	
def getWholeExpertsSetsFromPickle(noFolds,expertsRatingsDBTable):
	expertSet={}
	for index in range(1,noFolds+1):
		file=expertsRatingsDBTable+".pickle"
		with open(path + "pickle/"+ file, 'rb') as handle:
			expertSet = pickle.load(handle)
		print "fetched expert set from file = ",file, " len of expertSet = ",len(expertSet), " added to list "
	return expertSet	

	
def filterExpertsWithMinRatings(expertsDict,expertSetList,noFolds,minRating):
	for fold in range(0,noFolds):
	 	expertSet 	= 	expertSetList[fold]
	 	filteredSet = 	{}
	 	for expertId,expertRatings in expertSet.items():
	 		if getMinRatingExpert(expertsDict,expertId) >= minRating:
	 			filteredSet[expertId] = expertRatings
	 	expertSetList[fold]=filteredSet
	return expertSetList
# this fn gets all the training data for all CV folds from the db and stores locally in pickle files
def saveTrainingSetsToPickle(noFolds):
	for index in range(1,noFolds+1):
		dbTable=trainUsersDBTableName+str(index)
		trainSet=getTrainRatingsFromDB(dbTable)
		file=trainUsersDBTableName+str(index)+".pickle"
		if debug:
			print "no of ratings in train set = ",len(trainSet), "saved to file =",file
		with open(path +"pickle/" + file, 'wb') as handle:
			pickle.dump(trainSet, handle, protocol=pickle.HIGHEST_PROTOCOL)

# this fn fetches all the training sets for all CV folds stored in pickle and constructs python dictionaries from them
def getTrainingSetsFromPickle(noFolds):
	trainSetList=[]
	for index in range(1,noFolds+1):
		file=trainUsersDBTableName+str(index)+".pickle"
		with open(path + "pickle/" + file, 'rb') as handle:
			#noRatings,trainSet = pickle.load(handle)
			trainSet = pickle.load(handle)
		trainSetList.append(trainSet)
		if debug>1:
			print "fetched train set from file = ",file, " no of training users = ",len(trainSet)
	return trainSetList
def saveValidationSetsToPickle(noFolds):
	for index in range(1,noFolds+1):
		dbTable=valUsersDBTableName+str(index)
		valSet=getTestRatingsFromDB(dbTable)
		file=valUsersDBTableName+str(index)+".pickle"
		if debug>1:
			print "no of ratings in val set = ",len(valSet), "saved to file = ",file
		with open(path + "pickle/" + file, 'wb') as handle:
			pickle.dump(valSet, handle, protocol=pickle.HIGHEST_PROTOCOL)
# this fn fetches all the validation sets for all CV folds stored in pickle and constructs python dictionaries from them
def getValidationSetsFromPickle(noFolds):
	valSetList=[]
	for index in range(1,noFolds+1):
		file=valUsersDBTableName+str(index)+".pickle"
		with open(path + "pickle/" + file, 'rb') as handle:
			valSet = pickle.load(handle)
		valSetList.append(valSet)
		if debug>1:
			print "fetched validation set from file = ",file, " len of val set = ",len(valSet), " added to list "
	return valSetList	
	
def calcSimilarity(userId1,user1Ratings,userId2,user2Ratings): 

	noItemsUser1	=	len(user1Ratings)
	noItemsUser2	=	len(user2Ratings)
	
	#print "start calcSimilarity ",userId1, "has rated ",noItemsUser1,"items and ",userId2, "has rated ",noItemsUser2, "items"
	mutuallyRatedItems		={}    
	simScore 				=0.0   
	noMutuallyRatedItems	=0.0
	sumProdBothRatings		=0.0
	sumSqRatingsUser1		=0.0
	sumSqRatingsUser2		=0.0
	
	for itemId in user1Ratings.keys():
		if itemId in user2Ratings.keys():
			#print "Mutually rated ",getItemName(itemId),"ratings are",user1Ratings[itemId],"and",user2Ratings[itemId]
			noMutuallyRatedItems	=	noMutuallyRatedItems	+1
			sumProdBothRatings 	=	sumProdBothRatings 	+ 	(user1Ratings[itemId]*user2Ratings[itemId])
			sumSqRatingsUser1 	= 	sumSqRatingsUser1 	+ 	pow(user1Ratings[itemId] ,2)
			sumSqRatingsUser2 	= 	sumSqRatingsUser2 	+ 	pow(user2Ratings[itemId] ,2)
			
	numerator				=	sumProdBothRatings*2*noMutuallyRatedItems
	denominator				=	sqrt(sumSqRatingsUser1) *sqrt(sumSqRatingsUser2) * (noItemsUser1+noItemsUser2)

	if not noMutuallyRatedItems:
		#print "end 1 calcSimilarity simScore  =",simScore
		return 0.0
	if not denominator:
		#print "end 2 calcSimilarity simScore  =",simScore
		return 0.0

	simScore	=	numerator/denominator
	#print "end 3 calcSimilarity simScore  =",simScore
	return simScore


def calcSimilarityUserExpert(userId,userRatings,expertId,expertRatings): 

	noItemsUser		=	len(userRatings)
	noItemsExpert	=	len(expertRatings)
	mutuallyRatedItems		={}    
	simScore 				=0.0   
	noMutuallyRatedItems	=0.0
	sumProdBothRatings		=0.0
	sumSqRatingsUser		=0.0
	sumSqRatingsExpert		=0.0
	
	for userItemId in userRatings.keys(): #k, "21hqlWgjmNgW4pBZJDuziQ" v 2
		expertItemId = userItemId.replace('\"','')
		if expertItemId in expertRatings.keys():
			noMutuallyRatedItems	=	noMutuallyRatedItems	+	1
			sumProdBothRatings 	=	sumProdBothRatings 	+ 	(userRatings[userItemId]*expertRatings[expertItemId][0])
			sumSqRatingsUser 	= 	sumSqRatingsUser 	+ 	pow(userRatings[userItemId] ,2)
			sumSqRatingsExpert 	= 	sumSqRatingsExpert 	+ 	pow(expertRatings[expertItemId][0] ,2)
			
	numerator				=	sumProdBothRatings*2*noMutuallyRatedItems
	denominator				=	sqrt(sumSqRatingsUser) * sqrt(sumSqRatingsExpert) * (noItemsUser+noItemsExpert)

	if not noMutuallyRatedItems:
		return 0.0
	if not denominator:
		return 0.0

	simScore	=	numerator/denominator
	return simScore
	
def calcPearson(userId1,user1Ratings,userId2,user2Ratings): 
	noItemsUser1	=	len(user1Ratings)
	noItemsUser2	=	len(user2Ratings)
	
	if debug >2:
		print userId1, "has rated ",noItemsUser1,"items and ",userId2, "has rated ",noItemsUser2, "items"
	mutualItems		={}    
	simScore 				=0.0   
	noMutuallyRatedItems	=0.0
	sumProdBothRatings		=0.0
	sumSqRatingsUser1		=0.0
	sumSqRatingsUser2		=0.0
	sumUser1				=0.0
	sumUser2				=0.0
	numerator				=0.0
	denominator				=0.0
	
	for itemId in user1Ratings.keys():
		if itemId in user2Ratings.keys():
			if debug >2:
				print "Mutually rated ",getItemName(itemId),"ratings are",user1Ratings[itemId],"and",user2Ratings[itemId]
			mutualItems[itemId]=1

	noMutuallyRatedItems	=	len(mutualItems)
	if not noMutuallyRatedItems:
		return 0.0
		
	sumUser1Ratings 		=	sum([user1Ratings[item] for item in mutualItems])
	sumUser2Ratings 		=	sum([user2Ratings[item] for item in mutualItems])
	
	sumSqUser1Ratings		=	sum([pow(user1Ratings[item],2) for item in mutualItems])
	sumSqUser2Ratings		=	sum([pow(user2Ratings[item],2) for item in mutualItems])
	
	sumProds				=	sum([user1Ratings[item]*user2Ratings[item] for item in mutualItems])
	
	numerator				=	sumProds-((sumUser1Ratings*sumUser2Ratings) / noMutuallyRatedItems)
	denominator				=	sqrt((sumSqUser1Ratings-pow(sumUser1Ratings,2)/noMutuallyRatedItems) * (sumSqUser2Ratings-pow(sumUser2Ratings,2)/noMutuallyRatedItems))


	if not denominator:
		return 0.0
 
	simScore	=	numerator/denominator
	return simScore
	
def calcPearson2(userId1,user1Ratings,userId2,user2Ratings): 
	noItemsUser1	=	len(user1Ratings)
	noItemsUser2	=	len(user2Ratings)
	
	if debug >2:
		print userId1, "has rated ",noItemsUser1,"items and ",userId2, "has rated ",noItemsUser2, "items"
	mutuallyRatedItems		={}    
	simScore 				=0.0   
	noMutuallyRatedItems	=0.0
	sumProdBothRatings		=0.0
	sumSqRatingsUser1		=0.0
	sumSqRatingsUser2		=0.0
	sumUser1				=0.0
	sumUser2				=0.0
	
	for itemId in user1Ratings.keys():
		if itemId in user2Ratings.keys():
			if debug >2:
				print "Mutually rated ",getItemName(itemId),"ratings are",user1Ratings[itemId],"and",user2Ratings[itemId]
			mutuallyRatedItems[itemId]=1
			sumProdBothRatings 	=	sumProdBothRatings 	+ 	(user1Ratings[itemId]*user2Ratings[itemId])
			sumSqRatingsUser1 	= 	sumSqRatingsUser1 	+ 	pow(user1Ratings[itemId] ,2)
			sumSqRatingsUser2 	= 	sumSqRatingsUser2 	+ 	pow(user2Ratings[itemId] ,2)
			sumUser1			=	sumUser1			+	user1Ratings[itemId]
			sumUser2			=	sumUser2			+	user2Ratings[itemId]
			
	noMutuallyRatedItems	=	len(mutuallyRatedItems)
	if not noMutuallyRatedItems:
		return 0.0
		
	numerator				=	sumProdBothRatings-((sumUser1*sumUser2) / noMutuallyRatedItems)
	denominator				=	sqrt((sumSqRatingsUser1-pow(sumUser1,2)/noMutuallyRatedItems) * (sumSqRatingsUser2-pow(sumUser2,2)/noMutuallyRatedItems))


	if not denominator:
		return 0.0

	print 
	simScore	=	numerator/denominator
	return simScore
	
def calcEuclideanDistance(userId1,user1Ratings,userId2,user2Ratings): 

	noItemsUser1	=	len(user1Ratings)
	noItemsUser2	=	len(user2Ratings)
	
	if debug >2:
		print userId1, "has rated ",noItemsUser1,"items and ",userId2, "has rated ",noItemsUser2, "items"
	mutuallyRatedItems		={}    
	simScore 				=0.0  
	noMutuallyRatedItems	=0.0
	sumSquares				=0.0
	
	for itemId in user1Ratings.keys():
		if itemId in user2Ratings.keys():
			if debug >2:
				print "Mutually rated item = ",itemId,"ratings are ", user1Ratings[itemId],"and ",user2Ratings[itemId]				
			mutuallyRatedItems[itemId]=1
			sumSquares = sumSquares + pow((user1Ratings[itemId]-user2Ratings[itemId]),2)
	noMutuallyRatedItems	=	len(mutuallyRatedItems)
	
	if not noMutuallyRatedItems:
		return 0
	
	simScore	=	1.0/(1.0+sumSquares)
	return simScore

def calcCosine(userId1,user1Ratings,userId2,user2Ratings): 

	noItemsUser1	=	len(user1Ratings)
	noItemsUser2	=	len(user2Ratings)
	
	if debug>1:
		print userId1, "has rated ",noItemsUser1,"items and ",userId2, "has rated ",noItemsUser2, "items"
	mutuallyRatedItems		={}    
	simScore 				=0.0  
	noMutuallyRatedItems	=0.0
	sumSquaresUser1			=0.0
	sumSquaresUser2			=0.0
	sumProdRatings			=0.0
	
	for itemId in user1Ratings.keys():
		if itemId in user2Ratings.keys():
			if debug>1:
				print "Mutually rated item = ",itemId,"ratings are ", user1Ratings[itemId],"and ",user2Ratings[itemId]				
			mutuallyRatedItems[itemId]=1
			sumProdRatings 		=	sumProdRatings	 +  (user1Ratings[itemId] * user2Ratings[itemId])
			sumSquaresUser1	    =   sumSquaresUser1  + pow(user1Ratings[itemId],2)
			sumSquaresUser2	    =   sumSquaresUser2  + pow(user2Ratings[itemId],2)
			
	noMutuallyRatedItems	=	len(mutuallyRatedItems)

	if not noMutuallyRatedItems:
		return 0
	if debug>1:
		print "noMutuallyRatedItems = ",noMutuallyRatedItems	
	numerator 		=	sumProdRatings
	denominator		=	sqrt(sumSquaresUser1) * sqrt(sumSquaresUser2)

	if not denominator:
		return 0
		
	simScore	=	numerator/denominator
	return simScore
			
def saveItemsAvgRatingExpertsDict():
	cur = db.cursor()			
	cur.execute("SELECT * FROM "+ expertsRatingsDBTable)
	numRatings	=0
	sumRatings	=0
	avgRating	=0
	ratingsDict	={}
	for row in cur.fetchall():
		itemId			=row[3]
		rating			=row[4]
		if itemId not in ratingsDict.keys():
			ratingsDict[itemId] = [rating,1]
		else:
			sumRatings,numRatings = ratingsDict[itemId]
			newSumRatings	=	sumRatings + rating
			newNumRatings	=	numRatings +	1
			ratingsDict[itemId]= [newSumRatings,newNumRatings]

	for itemId, itemValues in ratingsDict.items():
		if itemValues[0] and itemValues[1]:
			ratingsDict[itemId]=itemValues[0]/itemValues[1]
			#print "for itemId", itemId, "avg value is ", ratingsDict[itemId]
			
	with open(path + "pickle/"+ "itemAvg_"+expertsRatingsDBTable+".pickle", 'wb') as handle:
		pickle.dump(ratingsDict, handle, protocol=pickle.HIGHEST_PROTOCOL)

def saveItemsAvgRatingUsersDictList(foldNo):
	cur = db.cursor()			
	cur.execute("SELECT * FROM "+ trainUsersDBTableName+str(foldNo))
	numRatings	=0
	sumRatings	=0
	avgRating	=0
	ratingsDict	={}
	for row in cur.fetchall():
		itemId			=row[3].replace('\"','')
		rating			=row[4]
		if itemId not in ratingsDict.keys():
			ratingsDict[itemId] = [rating,1]
		else:
			sumRatings,numRatings = ratingsDict[itemId]
			newSumRatings	=	sumRatings + rating
			newNumRatings	=	numRatings +	1
			ratingsDict[itemId]= [newSumRatings,newNumRatings]

	for itemId, itemValues in ratingsDict.items():
		ratingsDict[itemId]=itemValues[0]/itemValues[1]
		
	with open(path + "pickle/"+ "itemAvg_"+trainUsersDBTableName+str(foldNo)+".pickle", 'wb') as handle:
		pickle.dump(ratingsDict, handle, protocol=pickle.HIGHEST_PROTOCOL)
	   	
	
##################################################################################################
'''
simMatrixList =[{},{},{},{},{}]
simEliteMatrixList =[{},{},{},{},{}]

helper.saveExpertsRatingsFromRtoPickle(5,"expert_ratings_all_")
helper.saveExpertsRatingsFromRtoPickle(5,"expert_ratings_250_")
helper.saveExpertsRatingsFromRtoPickle(5,"expert_ratings_500_")
helper.saveExpertsRatingsFromRtoPickle(5,"expert_ratings_750_")

saveExpertsRatingsAsCSV("train_ratings_val_1")
saveExpertsRatingsAsCSV("train_ratings_val_2")
saveExpertsRatingsAsCSV("train_ratings_val_3")
saveExpertsRatingsAsCSV("train_ratings_val_4")
saveExpertsRatingsAsCSV("train_ratings_val_5")

saveExpertsRatingsAsCSV("train_ratings_train_1")
saveExpertsRatingsAsCSV("train_ratings_train_2")
saveExpertsRatingsAsCSV("train_ratings_train_3")
saveExpertsRatingsAsCSV("train_ratings_train_4")
saveExpertsRatingsAsCSV("train_ratings_train_5")
'''

