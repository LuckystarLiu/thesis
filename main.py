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

path 						= '/Users/paridhi/thesis/code2/csv_data/' 
itemsTable			 		= "rests"
trainUsersRatingsTable		= "train_ratings"
trainUsersDBTableName 		= ""
valUsersDBTableName  		= ""
expertsRatingsDBTable		= ""
connector 					= pymysql # MySQLdb                 
db = connector.connect(host		="localhost",user="root", passwd="Pass1234",db="yelp_all")

simUserMatrixList				=	[{},{},{},{},{}]
simEliteMatrixList 				=	[{},{},{},{},{}]
#expertSetList					=	[{},{},{},{},{}]
expertSet						=	{}
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

def getminExpertRatingExpert(expertsDict,expertId):
	if expertId not in expertsDict.keys():
		expertId = expertId.replace('\"','')
		if expertId not in expertsDict.keys():
			print "Error : Populate expert dict first!"
			return
	[expertName,expertId,avgStars,reviewCount]	= expertsDict[expertId]
	return reviewCount
		
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
		
def getUserRatingsFromDict(userId,foldNo):
	trainSet	=	trainSetList[foldNo-1][1]
	userRatings = 	{}
	if userId in trainSet.keys():
		for key,value in trainSet[userId].items():
			userRatings[key] = value[0]
	return userRatings
	
def getExpertRatingsFromDict(expertId,foldNo):
	#expertSet		=	expertSetList[foldNo-1]
	expertRatings 	=	{}
	if expertId in expertSet.keys():
		for key,value in expertSet[expertId].items():
			expertRatings[key] = value[0]
	return expertRatings
	
def getKNNList(userId,k,foldNo):
	simDict = {}
	simMatrix=simUserMatrixList[foldNo-1]
	if userId not in simMatrix.keys():
		print "Error : Populate similarity matrix for userid =  ",userId, "first"
		return []
	simDict = simMatrix[userId]
	simList=sorted(simDict, key=simDict.get)
	return simList[0:k]

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

def populateExpertSimMatrix(expertSet,userId,userRatings,foldNo):	
	if len(userRatings) ==0:
		return
	simDict={}
	expertRatings={}
	score=0
	for expertId in expertSet.keys():
			expertRatings = expertSet[expertId]
			if len(expertRatings) > 0:
	    			score= calcSimilarityUserExpert(userId,userRatings,expertId,expertRatings)
			simDict[expertId]=score
	simEliteMatrix=simEliteMatrixList[foldNo-1]
	simEliteMatrix[userId] = simDict
	simEliteMatrixList[foldNo-1]=simEliteMatrix

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
	score=0
	trainSet=trainSetList[foldNo-1][1]
	for neighbourId in trainSet.keys():
			neighbourRatings = getUserRatingsFromDict(neighbourId,foldNo)#neighbourId =  "NrRS56Dv52m2BND-9v1qWw"
			if len(neighbourRatings) > 0:
	    			score= calcSimilarity(userId,userRatings,neighbourId,neighbourRatings)
			simDict[neighbourId]=score
	simMatrix=simUserMatrixList[foldNo-1]
	simMatrix[userId] = simDict
	#print "user matrix populated for ", userId, "sim dict size is ", len(simDict)
	simUserMatrixList[foldNo-1]=simMatrix

def initializeItemsAvgRatings(noCVFolds):
	itemsAvgUsersList=[{},{},{},{},{}]
	for index in range(0,noCVFolds):
		foldNo=index+1
		itemsAvgUsersList[index]=getItemsAvgRatingUsersDictList(foldNo)
	return itemsAvgUsersList
	
def getItemsAvgRatingExpertsDict():
	with open(path + "pickle/"+ "itemAvg_"+expertsRatingsDBTable+".pickle" , 'rb') as handle:
   		ratings = pickle.load(handle)
   	return ratings
   	
def getItemsAvgRatingUsersDictList(foldNo):
	with open(path + "pickle/"+ "itemAvg_"+trainUsersDBTableName+str(foldNo)+".pickle" , 'rb') as handle:
   		ratings = pickle.load(handle)
   	return ratings

def calcColdStartCases(noCVFolds):
	file	= open(path + "results_coldstart.csv", 'wb')
	writer 	= csv.writer(file)
	writer.writerow(["foldNo","noTestUsers","noNewUsers"])
	for foldNo in range(1,noCVFolds+1):
	
		testSet			=	valSetList[foldNo-1].values()
		noTestUsers		=	len(testSet)
		noNewUsers		=	0
		for testUserDetails in testSet:
			userId 		 = testUserDetails[1]
			userRatings=getUserRatingsFromDict(userId,foldNo) # userId is double quoted, so is the key itemId in userRatings, rating is an int value
			if len(userRatings):# user seen before in the system
					noNewUsers = noNewUsers +	1
		#print "calcColdStartCases for fold no = ", foldNo, "noTestUsers  = ",noTestUsers, "noNewUsers is = ", noNewUsers
		writer.writerow([foldNo,noTestUsers,noNewUsers])
   	
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
	for neighbourId in neighbourSet:
		neighbourRatings	=   getUserRatings(neighbourId,foldNo) # get the ratings for current neighbour
		neighbourRatingItem =	0
		simScore			=	0
		neighbourMeanRating	=	0		
		if neighbourId in simUserDict.keys():
			simScore	=	simUserDict[neighbourId]
		else:
		    simScore	= 	calcSimilarity(userId,userRatings,neighbourId,neighbourRatings)
		if itemId in neighbourRatings.keys() and simScore>0.0: # the current neighbour has already rated the item
			neighbourRatingItem			= 	neighbourRatings[itemId] # known rating for neighbour of item 
			simWeightedNeighbourRating	=	simWeightedNeighbourRating + (neighbourRatingItem)*simScore		
			sumSimScores				=	sumSimScores + simScore
		neighbourMeanRating 		=	 sum(neighbourRatings.values())/len(neighbourRatings)
	if sumSimScores ==0 or simWeightedNeighbourRating ==0: # sim weighted avg will be zero, return mean user rating
		return avgRating,coverage 
	coverage		=	(kNN)/totalNeighbours	
	predictedRating	=	simWeightedNeighbourRating/sumSimScores
	return predictedRating,coverage
	
def predictExpCFRating(userId,itemId,userRatings,foldNo,actualRating,simThreshold,confThreshold):
	coverage=1
	if itemId in itemsAvgExpertsDict.keys():	#itemsAvgExpertsList[foldNo-1].keys():
		avgRating= itemsAvgExpertsDict[itemId] # check is item avg rating is pre-calculated
	else:
		avgRating= helper.getAvgItemRatingFromExperts(itemId,foldNo,expertsRatingsDBTable) # make db call for avg rating of item
	if len(userRatings) == 0 : # cold-start	
		#print "Case 1 : Cold start return avgRating,avgRating,actualRating ",avgRating,avgRating,actualRating
		return avgRating,avgRating,coverage # for cold start, return avg item rating and full coverage	
	meanRatingUser		= 	sum(userRatings.values()) / len(userRatings)
	simExperDict	 	= 	simEliteMatrixList[foldNo-1][userId]
	totalExperts		=	len(expertSet)
	noExpertsUsed				=	0
	noExpertsRatedItem			=	0
	sumSimScores				=	0
	simWeightedExpertRating		=	0
	for expertId in expertSet.keys():
		expertRatings		=   expertSet[expertId] # get the ratings for current expert
		expertRatingItem 	=	0
		simScore			=	0
		expertMeanRating	=	0
		if expertId in simExperDict.keys():
			simScore	=	simExperDict[expertId]
		else:
			simScore	= 	calcSimilarityUserExpert(userId,userRatings,expertId,expertRatings)
		if simScore < simThreshold:
			continue # expert discarded since sim score is below threshold
		noExpertsUsed 		=	noExpertsUsed +1 
		if itemId.replace('\"','') in expertRatings.keys(): # the current expert has already rated the item
			noExpertsRatedItem 			= noExpertsRatedItem +1
			expertRatingItem			= expertRatings[itemId.replace('\"','')][0] # known rating for expert of item 	
			simWeightedExpertRating	=	 simWeightedExpertRating	+	(expertRatingItem)*simScore #(expertRatingItem-expertMeanRating)*simScore
			sumSimScores			=	 sumSimScores + simScore	
		sumRatings=0
		for value in expertRatings.values():
			sumRatings = sumRatings + value[0]			
		expertMeanRating 		=	 sumRatings/len(expertRatings)	
	if noExpertsRatedItem < confThreshold: # confidence threshold ,no prediction made, return user mean and full coverage
		#print "Case 2 : avgRating,meanRatingUser,actualRating",avgRating,meanRatingUser,actualRating
		return avgRating,meanRatingUser,coverage
	if sumSimScores ==0 or simWeightedExpertRating == 0: # sim weighted avg will be zero, return mean user rating
		#print "Case 5 : avgRating,meanRatingUser,actualRating",avgRating,meanRatingUser,actualRating
		return avgRating,meanRatingUser,coverage
	coverage		=	(noExpertsUsed/totalExperts)
	predictedRating	=	(simWeightedExpertRating/sumSimScores)
	#print "Case 4 : avgRating,predictedRating,actualRating",avgRating,predictedRating,actualRating
	return avgRating,predictedRating,coverage

		
def predictExpertCFRatingMod(expertSet,actualRating,userId,itemId,userRatings,foldNo,simThreshold,confThreshold):
	coverage=1
	if itemId in itemsAvgExpertsDict.keys():	#itemsAvgExpertsList[foldNo-1].keys():
		avgRating= itemsAvgExpertsDict[itemId] # check is item avg rating is pre-calculated
	else:
		avgRating= helper.getAvgItemRatingFromExperts(itemId,foldNo,expertsRatingsDBTable) # make db call for avg rating of item
		
	if len(userRatings) == 0 : # cold-start
		return avgRating,avgRating,coverage # for cold start, return avg item rating and full coverage	
	meanRatingUser	 = 	sum(userRatings.values()) / len(userRatings)
	simExperDict	 = 	simEliteMatrixList[foldNo-1][userId]
	totalExperts		=len(expertSet)
	noExpertsUsed		=0
	noExpertsRatedItem	=0
	sumSimScores		=0
	simWeightedExpertRating=0
	for expertId in expertSet.keys():
		expertRatings		=   expertSet[expertId] # get the ratings for current expert
		expertRatingItem 	=	0
		simScore			=	0
		expertMeanRating	=	0
		if expertId in simExperDict.keys():
			simScore	=	simExperDict[expertId]
		else:
			simScore	= 	helper.calcSimilarityUserExpert(userId,userRatings,expertId,expertRatings)
		if simScore < simThreshold:
			#print "--------------------- simScore = ", simScore, "fal"
			continue # expert discarded since sim score is below threshold
		noExpertsUsed 		=	noExpertsUsed +1 # this expert will be used for prediction
		if itemId.replace('\"','') in expertRatings.keys(): # the current expert has already rated the item
			noExpertsRatedItem 			= noExpertsRatedItem +1
			expertRatingItem			= expertRatings[itemId.replace('\"','')][0] # known rating for expert of item 		
		sumRatings=0
		for value in expertRatings.values():
			sumRatings = sumRatings + value[0]			
		expertMeanRating 		=	 sumRatings/len(expertRatings)
		simWeightedExpertRating	=	 simWeightedExpertRating	+	(expertMeanRating-expertRatingItem)*simScore #(expertRatingItem-expertMeanRating)*simScore
		sumSimScores			=	 sumSimScores + simScore
	if noExpertsRatedItem < confThreshold: # confidence threshold ,no prediction made, return user mean and full coverage
		return avgRating,meanRatingUser,coverage
	if sumSimScores ==0: # sim weighted avg will be zero, return mean user rating
		return avgRating,meanRatingUser,coverage
	coverage		=	(noExpertsUsed/totalExperts)
	#predictedRating	=	meanRatingUser + (simWeightedExpertRating/sumSimScores)
	predictedRating	=	meanRatingUser + (simWeightedExpertRating/sumSimScores)
	#print "userId", userId, "  itemId = ",itemId,"   actualRating", actualRating, "meanRatingUser",meanRatingUser,"simWeightedExpertRating",simWeightedExpertRating,"sumSimScores",sumSimScores
	return avgRating,predictedRating,coverage

def testExpertCF(expertSet,noCVFolds,simThreshold,confThreshold,minExpertRating,noTestUsers):	
	file	= open(path + "/details/results_summary.csv", 'wb')
	writer 	= csv.writer(file)
	writer.writerow(["foldNo","minExpertRating","noTestUsers","simThreshold","confThreshold","maeAvgExpCF","RMSEAvgExp","maeExpCF","RMSEExp"])
	print "foldNo,minExpertRating,noTestUsers,simThreshold,confThreshold,maeAvgExpCF,rMSEAvgExp,maeExpCF,rMSEExp"
	for foldNo in range(1,noCVFolds+1):
		#file2	= open(path + "/details/results_min_rating_"+str(minExpertRating)+"_fold_"+str(foldNo)+"_sim_"+str(simThreshold)+"_conf_"+str(confThreshold)+".csv", 'wb')
		#writer2 	= csv.writer(file2)
		#writer2.writerow(["foldNo","minExpertRating","simThreshold","confThreshold","newUser","userName","userId","itemName","itemId","actualRating","avgExpertRating","predictedExpertRating"])
		noUsersTested	=	0
		yAvgExpPred			=	[]
		yTrue				=	[]
		yExpPred			=	[]
		maeAvgExpCF			=	0
		maeExpCF			=	0
		rMSEAvgExp			=	0	
		rMSEExp				=	0
		testSet			=	valSetList[foldNo-1].values()
		for testUserDetails in testSet:
			newUser=1
			if noUsersTested > noTestUsers:
				break
			noUsersTested	=	noUsersTested + 1
			userName 	 = testUserDetails[0]
			userId 		 = testUserDetails[1]
			itemName 	 = testUserDetails[2]
			itemId 		 = testUserDetails[3]
			actualRating = testUserDetails[4]
			predictedExpertRating=	0
			avgExpertRating			=	0
			userRatings=getUserRatingsFromDict(userId,foldNo) # userId is double quoted, so is the key itemId in userRatings, rating is an int value
			if len(userRatings):# user seen before in the system
				newUser=0
				populateExpertSimMatrix(expertSet,userId,userRatings,foldNo)
			avgExpertRating,predictedExpertRating = predictExpCFRating(userId,itemId,userRatings,foldNo,actualRating,simThreshold,confThreshold)
			#writer2.writerow([foldNo,minExpertRating,simThreshold,confThreshold,newUser,userName.replace('\"',''),userId.replace('\"',''),itemName.replace('\"',''),itemId.replace('\"',''),actualRating,avgExpertRating,predictedExpertRating])
			yTrue.append(actualRating)
			yAvgExpPred.append(avgExpertRating)
			yExpPred.append(predictedExpertRating)
		maeAvgExpCF		=	calcMAE(yTrue, yAvgExpPred)
		maeExpCF		=	calcMAE(yTrue, yExpPred)
		rMSEAvgExp		=   calcRMSE(yTrue, yAvgExpPred)
		rMSEExp			=   calcRMSE(yTrue, yExpPred)
		writer.writerow([foldNo,minExpertRating,noTestUsers,simThreshold,confThreshold,maeAvgExpCF,rMSEAvgExp,maeExpCF,rMSEExp])
		print foldNo,minExpertRating,noTestUsers,simThreshold,confThreshold,maeAvgExpCF,rMSEAvgExp,maeExpCF,rMSEExp


tl=[]
t=0
sl=[]
el=[]
for i in range(0,20):
	t=t+0.05
	tl.append(round(t,2))

file	= open("/Users/paridhi/thesis/img20.csv", 'wb')
writer 	= csv.writer(file)
writer.writerow(["t","maeStd","maeExp"])

for t in tl:
	writer.writerow([t,s,e])
		
#----------------------------------------------------------------------------------------------------------------------------
'''
trainUsersDBTableName 	= "train_ratings_train_"
valUsersDBTableName  	= "train_ratings_val_"
debug=0
noCVFolds=5
noTestUsers=10
minRatingValues=[250]
path 					= '/home/ec2-user/code2/csv_data/'
trainSetList	=	helper.getTrainingSetsFromPickle(noCVFolds)
valSetList		=	helper.getValidationSetsFromPickle(noCVFolds)
'''
#for minRating in minRatingValues:
#expertsRatingsDBTable   = "ratings_experts_"+str(minRating)+"_rests"#"expert_ratings_all_"
#itemsAvgExpertsDict		= getItemsAvgRatingExpertsDict() # format is itemsAvgExpertsDict["AkOruz5CrCxUmXe1p_WoRg"]
#expertSet = helper.getWholeExpertsSetsFromPickle(noCVFolds,"expert_ratings_"+str(minRating))
#testExpertCF(expertSet,noCVFolds,simThreshold,confThreshold,minRating,noTestUsers)

#	for simThreshold in [0.005]:
#		for confThreshold in [20]:
	
