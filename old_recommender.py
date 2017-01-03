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
	    			score= helper.calcSimilarityUserExpert(userId,userRatings,expertId,expertRatings)
			simDict[expertId]=score
	simEliteMatrix=simEliteMatrixList[foldNo-1]
	simEliteMatrix[userId] = simDict
	simEliteMatrixList[foldNo-1]=simEliteMatrix

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
	    			score= helper.calcSimilarity(userId,userRatings,neighbourId,neighbourRatings)
			simDict[neighbourId]=score
	simMatrix=simUserMatrixList[foldNo-1]
	simMatrix[userId] = simDict
	print "user matrix populated for ", userId, "sim dict size is ", len(simDict)
	simUserMatrixList[foldNo-1]=simMatrix

def predictStandardCFRating(userId,itemId,userRatings,foldNo,simThreshold,kNN):
	coverage=1
	if len(userRatings) == 0 : # cold-start
		if itemId.replace('\"','') in itemsAvgUsersList[foldNo-1].keys():
			avgRating= itemsAvgUsersList[foldNo-1][itemId.replace('\"','')] # check is item avg rating is pre-calculated
		else:
			avgRating= helper.getAvgItemRatingFromUsers(itemId,foldNo) # make db call for avg rating of item
		return avgRating, coverage # for cold start, return avg item rating and full coverage	
	meanRatingUser		= 	sum(userRatings.values()) / len(userRatings)
	#simUserDict		 	= 	simUserMatrixList[foldNo-1][userId]
	neighbourSet		=	getKNNList(userId,kNN,foldNo)
	totalNeighbours		=	len(neighbourSet)
	noNeighboursUsed	=	0
	sumSimScores		=	0
	simWeightedNeighbourRating=0
	for neighbourId in neighbourSet:
		neighbourRatings	=   getUserRatingsFromDict(neighbourId,foldNo) # get the ratings for current neighbour
		neighbourRatingItem =	0
		simScore			=	0
		neighbourMeanRating	=	0		
		#if neighbourId in simUserDict.keys():
		#	simScore	=	simUserDict[neighbourId]
		#else:
		simScore	= 	helper.calcSimilarity(userId,userRatings,neighbourId,neighbourRatings)
		print "simScore ________________________________________________ ",simScore	
		#if simScore < simThreshold:
		#	continue # neighbour discarded since sim score is below threshold
		noNeighboursUsed 		=	noNeighboursUsed +1 # this neighbour will be used for prediction
		if itemId in neighbourRatings.keys(): # the current neighbour has already rated the item
			neighbourRatingItem			= neighbourRatings[itemId] # known rating for neighbour of item 
		neighbourMeanRating 		=	 sum(neighbourRatings.values())/len(neighbourRatings)
		simWeightedNeighbourRating	=	 simWeightedNeighbourRating + (neighbourRatingItem-neighbourMeanRating)*simScore
		sumSimScores				=	 sumSimScores + simScore

	if sumSimScores ==0: # sim weighted avg will be zero, return mean user rating
		return meanRatingUser,coverage
	coverage		=	noNeighboursUsed/totalNeighbours
	predictedRating	=	meanRatingUser + (simWeightedNeighbourRating/sumSimScores)
	return predictedRating,coverage
	  
# returns avg expert rating, predicted exp rating and predicted exp coverage 
def predictExpertCFRating(expertSet,actualRating,userId,itemId,userRatings,foldNo,simThreshold,confThreshold):
	coverage=1

	if itemId in itemsAvgExpertsDict.keys():	#itemsAvgExpertsList[foldNo-1].keys():
		avgRating= itemsAvgExpertsDict[itemId] # check is item avg rating is pre-calculated
	else:
		avgRating= helper.getAvgItemRatingFromExperts(itemId,foldNo,expertsRatingsDBTable) # make db call for avg rating of item
		
	if len(userRatings) == 0 : # cold-start
		return avgRating, avgRating,coverage # for cold start, return avg item rating and full coverage	
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
			continue # expert discarded since sim score is below threshold
		noExpertsUsed 		=	noExpertsUsed +1 # this expert will be used for prediction
		if itemId.replace('\"','') in expertRatings.keys(): # the current expert has already rated the item
			noExpertsRatedItem 			= noExpertsRatedItem +1
			expertRatingItem			= expertRatings[itemId.replace('\"','')][0] # known rating for expert of item 		
		sumRatings=0
		for value in expertRatings.values():
			sumRatings = sumRatings + value[0]			
		expertMeanRating 		=	 sumRatings/len(expertRatings)
		simWeightedExpertRating	=	 simWeightedExpertRating	+	(expertRatingItem-expertMeanRating)*simScore
		sumSimScores			=	 sumSimScores + simScore
	if noExpertsRatedItem < confThreshold: # confidence threshold ,no prediction made, return user mean and full coverage
		return avgRating,meanRatingUser,coverage
	if sumSimScores ==0: # sim weighted avg will be zero, return mean user rating
		return avgRating,meanRatingUser,coverage
	coverage		=	(noExpertsUsed/totalExperts)
	predictedRating	=	meanRatingUser + (simWeightedExpertRating/sumSimScores)
	#print "userId", userId, "  itemId = ",itemId,"   actualRating", actualRating, "meanRatingUser",meanRatingUser,"simWeightedExpertRating",simWeightedExpertRating,"sumSimScores",sumSimScores
	return avgRating,predictedRating,coverage
	
def predictExpertCFRatingMod(expertSet,actualRating,userId,itemId,userRatings,foldNo,simThreshold,confThreshold):
	coverage=1

	if itemId in itemsAvgExpertsDict.keys():	#itemsAvgExpertsList[foldNo-1].keys():
		avgRating= itemsAvgExpertsDict[itemId] # check is item avg rating is pre-calculated
	else:
		avgRating= helper.getAvgItemRatingFromExperts(itemId,foldNo,expertsRatingsDBTable) # make db call for avg rating of item
		
	if len(userRatings) == 0 : # cold-start
		return avgRating, avgRating,coverage # for cold start, return avg item rating and full coverage	
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
	predictedRating		=	(simWeightedExpertRating/sumSimScores)
	#print "userId", userId, "  itemId = ",itemId,"   actualRating", actualRating, "meanRatingUser",meanRatingUser,"simWeightedExpertRating",simWeightedExpertRating,"sumSimScores",sumSimScores
	return avgRating,predictedRating,coverage
						
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
   	
def calcMAE(ytrue,ypred):
 	noTestCases = len(ytrue)
	if noTestCases:
		sumMAE	=	0.0
		for i in range(0,len(ytrue)):
			sumMAE = sumMAE + ytrue[i]-ypred[i]		
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
 	
def testExpertCF(expertSet,writer,noCVFolds,simThresholdValues,confThresholdValues,minExpertRating,noTestUsers):	
	for foldNo in range(1,noCVFolds+1):
		file2	= open(path + "results/results_all_folds.csv", 'wb')
		file3 	= open(path + "results/details/results_foldNo" + str(foldNo) +"_minExpertRating"+str(minExpertRating)+ ".csv", 'wb')
		writer2 	= csv.writer(file2)
		writer3 	= csv.writer(file3)
		writer2.writerow(["foldNo","minExpertRating","simThreshold","confThreshold","userName","userId","itemName","itemId","newUser","expertCoverage","expertModCoverage","actualRating", "avgExpertRating" , "predictedExpertRating","predictedExpertRatingMod"])
		writer3.writerow(["foldNo","minExpertRating","simThreshold","confThreshold","userName","userId","itemName","itemId","newUser","expertCoverage","expertModCoverage","actualRating", "avgExpertRating" ,"predictedExpertRating","predictedExpertRatingMod"])
		noUsersTested	=	0
		yAvgExpPred			=	[]
		yTrue				=	[]
		yExpPred			=	[]
		yExpModPred			=	[]
		expCoverageList		=	[]
		expModCoverageList	=	[]
		maeAvgExpCF			=	0
		maeExpCF			=	0
		maeExpModCF			=	0
		meanExpCoverage		=	0
		meanExpModCoverage	=	0
		rMSEAvgExp			=	0	
		rMSEExp				=	0
		rMSEExpMod			=	0
		
		testSet			=	valSetList[foldNo-1].values()
		for simThreshold in simThresholdValues:
			for confThreshold in confThresholdValues:					
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
					predictedExpertRating	=	0
					predictedExpertModRating=	0
					avgExpertRating			=	0
					expertCoverage			=	0	
					expertModCoverage		=	0
								
					userRatings=getUserRatingsFromDict(userId,foldNo) # userId is double quoted, so is the key itemId in userRatings, rating is an int value
					if len(userRatings):# user seen before in the system
						newUser=0
						populateExpertSimMatrix(expertSet,userId,userRatings,foldNo)
					avgExpertRating, predictedExpertRating,expertCoverage 			= predictExpertCFRating(expertSet,actualRating,userId,itemId,userRatings,foldNo,simThreshold,confThreshold)
					avgExpertRating, predictedExpertModRating,expertModCoverage 	= predictExpertCFRatingMod(expertSet,actualRating,userId,itemId,userRatings,foldNo,simThreshold,confThreshold)
					
					#print "actualRating, predictedExpertRating, predictedExpertModRating 	",actualRating, "	", predictedExpertRating,"	",predictedExpertModRating
					writer2.writerow([foldNo,minExpertRating,simThreshold,confThreshold,userName.replace('\"',''),userId.replace('\"',''),itemName.replace('\"',''),itemId.replace('\"',''),newUser,expertCoverage,expertModCoverage,actualRating,avgExpertRating,predictedExpertRating,predictedExpertModRating])
					writer3.writerow([foldNo,minExpertRating,simThreshold,confThreshold,userName.replace('\"',''),userId.replace('\"',''),itemName.replace('\"',''),itemId.replace('\"',''),newUser,expertCoverage,expertModCoverage,actualRating,avgExpertRating,predictedExpertRating,predictedExpertModRating])						
					yTrue.append(actualRating)
					yAvgExpPred.append(avgExpertRating)
					yExpPred.append(predictedExpertRating)
					yExpModPred.append(predictedExpertModRating)
					expCoverageList.append(expertCoverage)
					expModCoverageList.append(expertModCoverage)
					
				maeAvgExpCF		=	calcMAE(yTrue, yAvgExpPred)
				maeExpCF		=	calcMAE(yTrue, yExpPred)
				maeExpModCF		=	calcMAE(yTrue, yExpModPred)
				
				meanExpCoverage		=	sum(expCoverageList)/len(expCoverageList)
				meanExpModCoverage	=	sum(expModCoverageList)/len(expModCoverageList)
				
				rMSEAvgExp		=   calcRMSE(yTrue, yAvgExpPred)
				rMSEExp			=   calcRMSE(yTrue, yExpPred)
				rMSEExpMod		=   calcRMSE(yTrue, yExpModPred)
				
				#print "for CV fold no = ", foldNo,"simThreshold = ",simThreshold,"confThreshold = ",confThreshold,"minExpertRating = ",minExpertRating
				#print "minExpertRating = ", minExpertRating,"simThreshold = ",simThreshold,"confThreshold = ", confThreshold, "maeAvgExpCF = ",maeAvgExpCF,"rMSEAvgExp = ",rMSEAvgExp,"maeExpCF = ",maeExpCF,"rMSEExp = ",rMSEExp,"meanExpCoverage",meanExpCoverage
				writer.writerow([foldNo,minExpertRating,simThreshold,confThreshold,maeAvgExpCF,rMSEAvgExp,maeExpCF,rMSEExp,meanExpCoverage,maeExpModCF,rMSEExpMod,meanExpModCoverage])
   	  	   			
def predictRecommendations(writer,noCVFolds,minRating):
		
		noTestUsers=1000
		simThreshold=0
		simThresholdValues=[]
		for i in range(0,49):
			simThreshold=simThreshold+0.005
			simThresholdValues.append(round(simThreshold,4))

		confThresholdValues=[i for i in range(0,20,2)] #[1,10,20]
		expertSet = helper.getWholeExpertsSetsFromPickle(noCVFolds,"expert_ratings_"+str(minRating))
		testExpertCF(expertSet,writer,noCVFolds,simThresholdValues,confThresholdValues,minRating,noTestUsers)

debug=0
noCVFolds=5
trainUsersDBTableName 	= "train_ratings_train_"
valUsersDBTableName  	= "train_ratings_val_"
#path 					= '/home/ec2-user/code2/csv_data/'

trainSetList	=	helper.getTrainingSetsFromPickle(noCVFolds)
valSetList		=	helper.getValidationSetsFromPickle(noCVFolds)

for minRating in [750]:
	file 	= open(path + "results/results_summary"+str()+".csv", 'wb')
	writer 	= csv.writer(file)
	writer.writerow(["foldNo","minExpertRating","simThreshold","confThreshold","maeAvgExpCF","RMSEAvgExp","maeExpCF","RMSEExp","meanExpCoverage","maeExpModCF","RMSEExpMod","meanExpModCoverage"])
	expertsRatingsDBTable   = "ratings_experts_"+str(minRating)+"_rests"#"expert_ratings_all_"
	itemsAvgExpertsDict		= getItemsAvgRatingExpertsDict() # format is itemsAvgExpertsDict["AkOruz5CrCxUmXe1p_WoRg"]
	predictRecommendations(writer,noCVFolds,minRating)

#userid = "\"fNI8lbbxwF9xDYSkNTwThQ\""
#set=trainSetList[0][1]# also, the ratings will be a list not a int
#print set[userid]
#output k "fNI8lbbxwF9xDYSkNTwThQ" v  {'"xbCnAP4IsQrRBnHaBi-syw"': [5], '"Pg8OPh1D2ws0xO-I-8ppoA"': [5]}

#userid = "tnArVArlj5usJLZbq9ydbQ"
#set2=valSetList[0]
#print len(set2)#[userid]
#k "ctI_F912RzA1veRRJULWWg","SQHO1U7XsmuEAKX1j7yIMA" v  ['"Sunshine"', '"ctI_F912RzA1veRRJULWWg"', '"Mexi-Casa"', '"SQHO1U7XsmuEAKX1j7yIMA"', 2]


#trainUsersDBTableName 	= "small_train_ratings_train_"
#valUsersDBTableName  	= "small_train_ratings_val_"
#expertsRatingsDBTable = "expert_ratings_500_"

