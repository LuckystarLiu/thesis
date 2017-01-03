# /bin/python

from __future__ import division
#from sklearn.metrics import mean_absolute_error
#from sklearn.metrics import mean_squared_error
import random
import csv
import pymysql
import unicodedata
from math import sqrt
import datetime
import pickle
import time
#import numpy as np
import helper
import math

path 						= '/Users/paridhi/thesis/code2/csv_data/' 
itemsTable			 		= "rests"
trainUsersRatingsTable		= "train_ratings"
trainUsersDBTableName 		= ""
valUsersDBTableName  		= ""
expertsRatingsDBTable		= ""
connector 					= pymysql # MySQLdb                 
db = connector.connect(host		="localhost",user="root", passwd="Pass1234",db="yelp_all")

simUserMatrixList				=	[{},{},{},{},{}]
trainSetList					=	[{},{},{},{},{}]
valSetList						=	[{},{},{},{},{}]

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
		
def getAllItemsFromDB():
	try:
		cur = db.cursor()
		cur.execute("SELECT * FROM "+itemsTable)
		itemsList = []
		for row in cur.fetchall():
			itemsList.append(row[1])
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
		
def getUserRatings(userId,foldNo):
	trainSet	=	trainSetList[foldNo-1]
	ratings = 	{}
	userRatings = {}
	for key,val in trainSet.items():
		if userId == key:
			userRatings=val
	for k,v in userRatings.items():
			ratings[k] = v[0]
	return ratings
	
def getKNNList(userId,kNN,foldNo):
	simDict = {}
	simMatrix=simUserMatrixList[foldNo-1]
	kNNList=[]
	if userId not in simMatrix.keys():
		print "Error : Populate similarity matrix for userid =  ",userId, "first"
		return []
	simDict = simMatrix[userId]
	simList=sorted(simDict, key=simDict.__getitem__)
	for val in reversed(simList):
	     if kNN==0:
	     	break;
	     kNNList.append(val)
	     kNN=kNN-1
	return kNNList

def getRecLists(userId,userRating,tao):
   	recStdList=[]
    	recExpList=[]
	
	itemsList =getAllItemsFromDB()
	if debug ==1:
		print "No of items fetched from db = ",len(itemsList)
	count=0
	for itemId in itemsList:
		print itemId
		count = count +1
		if count == 10:
			break
		
    	stdRating = predictStandardCFRating(userId,itemId,userRating,50)
    	if stdRating >= tao:
    			recStdList.append(itemId)
    	avgRating, expertRating, expCoverage = predictExpertCFRating(expertSet,actualRating,userId,itemId,userRating,0.01,10)
    	if expertRating >= tao:
    			recExpList.append(itemId)
    	return recStdList,recExpList

def calcSimilarity(userId1,user1Ratings,userId2,user2Ratings): 
	noItemsUser1	=	len(user1Ratings)
	noItemsUser2	=	len(user2Ratings)
	mutuallyRatedItems		={}    
	simScore 				=0.0   
	noMutuallyRatedItems	=0.0
	sumProdBothRatings		=0.0
	sumSqRatingsUser1		=0.0
	sumSqRatingsUser2		=0.0
	for itemId in user1Ratings.keys():
		if itemId in user2Ratings.keys():
			rating1	=user1Ratings[itemId]#[0]
			rating2	=user2Ratings[itemId]#[0]
			noMutuallyRatedItems	=	noMutuallyRatedItems	+1
			sumProdBothRatings 	=	sumProdBothRatings 	+ 	(rating1*rating2)
			sumSqRatingsUser1 	= 	sumSqRatingsUser1 	+ 	pow(rating1,2)
			sumSqRatingsUser2 	= 	sumSqRatingsUser2 	+ 	pow(rating2,2)
	numerator				=	sumProdBothRatings*2*noMutuallyRatedItems
	denominator				=	sqrt(sumSqRatingsUser1) *sqrt(sumSqRatingsUser2) * (noItemsUser1+noItemsUser2)
	if not noMutuallyRatedItems:
		return 0.0
	if not denominator:
		return 0.0
	simScore	=	numerator/denominator
	return simScore

def populateUserSimMatrix(userId,userRatings,foldNo):
	if len(userRatings) ==0:
		return {}
	simDict={}
	neighbourRatings={}
	score=0.0
	trainSet=trainSetList[foldNo-1]
	for neighbourId in trainSet.keys():
			neighbourRatings = getUserRatings(neighbourId,foldNo)
			if len(neighbourRatings) > 0:
	    			score= calcSimilarity(userId,userRatings,neighbourId,neighbourRatings)
			simDict[neighbourId]=score
	simMatrix=simUserMatrixList[foldNo-1]
	simMatrix[userId] = simDict
	simUserMatrixList[foldNo-1]=simMatrix

def initializeItemsAvgRatings(noCVFolds):
	itemsAvgUsersList=[{},{},{},{},{}]
	overAllItemsAvgList=[]
	for index in range(0,noCVFolds):
		foldNo=index+1
		itemsAvgUsersList[index]=	getItemsAvgRatingUsersDictList(foldNo)
		overAllAvgItemRating 	=	sum(itemsAvgUsersList[index].values())/len(itemsAvgUsersList[index])
		overAllItemsAvgList.append(overAllAvgItemRating)
	return itemsAvgUsersList, overAllItemsAvgList

		
def getItemsAvgRatingUsersDictList(foldNo):
	with open(path + "pickle/"+ "itemAvg_"+trainUsersDBTableName+str(foldNo)+".pickle" , 'rb') as handle:
   		ratings = pickle.load(handle)
   	return ratings
def calcMAE(ytrue,ypred):
 	noTestCases = len(ytrue)
	if noTestCases:
		sumMAE	=	0.0
		for i in range(0,len(ytrue)):
			sumMAE = sumMAE + abs(ypred[i]-ytrue[i])
		return sumMAE/noTestCases
	else:
		return 0.0

def calcRMSE(ytrue,ypred):
 	noTestCases = len(ytrue)
 	sumSqError		=	0.0
	meanSumSqError	=	0.0
	rmse			=	0.0
	if noTestCases:
		for i in range(0,len(ytrue)):
			sumSqError = sumSqError + pow(ytrue[i]-ypred[i]	,2)
		meanSumSqError	=	sumSqError/noTestCases
		rmse			=	sqrt(meanSumSqError)
		return rmse
 	return rmse
 	
def predictStandardCFRating(userId,itemId,userRatings,foldNo,kNN,actualRating):
	coverage=1
	if itemId.replace('\"','') in itemsAvgUsersList[foldNo-1].keys():
		avgRating	= itemsAvgUsersList[foldNo-1][itemId.replace('\"','')] # check is item avg rating is pre-calculated
	else:
		avgRating	= helper.getAvgItemRatingFromUsers(itemId,foldNo) # make db call for avg rating of item
	if len(userRatings) == 0 : # cold-start	
		return avgRating, coverage # for cold start, return avg item rating and full coverage	
	meanRatingUser		= 	sum(userRatings.values()) / len(userRatings)
	simUserDict		 	= 	simUserMatrixList[foldNo-1][userId]
	neighbourSet		=	getKNNList(userId,kNN,foldNo)
	totalNeighbours		=	len(trainSetList[0])
	noNeighboursUsed	=	0
	sumSimScores		=	0
	simWeightedNeighbourRating=0
	#print "+------------------------------------------------------------------------------------------------------------------------------------------------+"
	#print "meanRatingUser = ",meanRatingUser, "actualRating",actualRating, "avgRating",avgRating,"overall",overAllItemsAvgList[foldNo-1]
	for neighbourId in neighbourSet:
		neighbourRatings	=   getUserRatings(neighbourId,foldNo) # get the ratings for current neighbour
		neighbourRatingItem =	0
		simScore			=	0
		neighbourMeanRating	=	0		
		if neighbourId in simUserDict.keys():
			simScore	=	simUserDict[neighbourId]
		else:
		    simScore	= 	helper.calcSimilarity(userId,userRatings,neighbourId,neighbourRatings)
		if itemId in neighbourRatings.keys() and simScore>0.0: # the current neighbour has already rated the item
			neighbourRatingItem			= 	neighbourRatings[itemId] # known rating for neighbour of item 
			simWeightedNeighbourRating	=	simWeightedNeighbourRating + (neighbourRatingItem)*simScore		
			sumSimScores				=	sumSimScores + simScore
			#print "-------------------------onlt ","simWeightedNeighbourRating",simWeightedNeighbourRating,"sumSimScores",sumSimScores
		neighbourMeanRating 		=	 sum(neighbourRatings.values())/len(neighbourRatings)
 		#if itemId in neighbourRatings.keys():
 			#print "Found!!!!!item rated by neighbour rating=",neighbourRatingItem, "simscore is", simScore, "neighbourMeanRating = ",neighbourMeanRating 
		#simWeightedNeighbourRating	=	 simWeightedNeighbourRating + (neighbourRatingItem-neighbourMeanRating)*simScore
	if sumSimScores ==0 or simWeightedNeighbourRating ==0: # sim weighted avg will be zero, return mean user rating
		#print "2. userId",userId,"kNN",kNN,"totalNeighbours",totalNeighbours, "meanRatingUser",meanRatingUser
		return avgRating,coverage 
	coverage		=	(kNN)/totalNeighbours	
	predictedRating	=	simWeightedNeighbourRating/sumSimScores
	#predictedRating	=	meanRatingUser + simWeightedNeighbourRating/sumSimScores
	#print "3. simWeightedNeighbourRating",simWeightedNeighbourRating,totalNeighbours,"sumSimScores",sumSimScores, "predictedRating",predictedRating
	return predictedRating,coverage
		  						

def testStdCFKNN(foldNo,kNNValues,noTestUsers):
	file	= open(path + "results_vary_knn_"+str(foldNo)+"_users_"+str(noTestUsers)+".csv", 'wb')
	writer 	= csv.writer(file)
	writer.writerow(["foldNo","kNN","noTestUsers","maeStdCF","meanStdCoverage","rMSEStd"])
	print "foldNo,kNN,noTestUsers,maeStdCF,meanStdCoverage,rMSEStd"	
	testSet			=	valSetList[foldNo-1].values()
	trainSet		=	trainSetList[foldNo-1]
	for kNN in kNNValues:
		yTrue				=	[]
		yStdPred			=	[]
		stdCoverageList		=	[]
		maeStdCF			=	0.0
		meanStdCoverage		=	0.0	
		rMSEStd				=	0.0				
		predictedStdRating	=	0.0
		stdCoverage			=	0.0
		noUsersTested		=	0
		for testUserDetails in testSet:	
			if noUsersTested > noTestUsers:
				break					
			userId 		 = testUserDetails[1]#		if userId.replace('\"','') == "DrWLhrK8WMZf7Jb-Oqc7ww":
			userName 	 = testUserDetails[0]
			itemName 	 = testUserDetails[2]
			itemId 		 = testUserDetails[3]
			actualRating = testUserDetails[4]
			userRatings	 = getUserRatings(userId,foldNo)
			 
			if len(userRatings):
				noUsersTested	=noUsersTested 	+	1
				populateUserSimMatrix(userId,userRatings,foldNo)
				predictedStdRating,stdCoverage 	= predictStandardCFRating(userId,itemId,userRatings,foldNo,kNN,actualRating)
				yTrue.append(actualRating)
				yStdPred.append(predictedStdRating)
				stdCoverageList.append(stdCoverage)	
				#print kNN,userId, actualRating,predictedStdRating,stdCoverage		
												
		maeStdCF		=	calcMAE(yTrue, yStdPred)
		meanStdCoverage	=	sum(stdCoverageList)/len(stdCoverageList)
		rMSEStd			=   calcRMSE(yTrue, yStdPred)
		print foldNo,kNN,noTestUsers,maeStdCF,meanStdCoverage,rMSEStd	
		writer.writerow([foldNo,kNN,noTestUsers,maeStdCF,meanStdCoverage,rMSEStd])	
debug=0
noCVFolds=5
noTestUsers=100
kNNValues = [i for i in range(0,50,1)]
expertsRatingsDBTable = "expert_ratings_500_"
trainUsersDBTableName 	= "train_ratings_train_"
valUsersDBTableName  	= "train_ratings_val_"
path 					= '/home/ec2-user/code2/csv_data/'
itemsAvgUsersList,overAllItemsAvgList 	= initializeItemsAvgRatings(noCVFolds)
fullTrainSetList		=	helper.getTrainingSetsFromPickle(noCVFolds)
valSetList				=	helper.getValidationSetsFromPickle(noCVFolds)
fullTrainLen			=	len(fullTrainSetList[0][1])
noTrainUsers = 5000#fullTrainLen #

for index in range(0,noCVFolds):
	count=0
	trainSet=fullTrainSetList[index][1]
	smallTrainSet ={}
	for k,v in trainSet.items():
        	if count>noTrainUsers:
        		break
    	        count=count+1
    	        smallTrainSet[k]=v
	trainSetList[index]=smallTrainSet
#for foldNo in range(1,noCVFolds+1):
testStdCFKNN(5,kNNValues,noTestUsers)

