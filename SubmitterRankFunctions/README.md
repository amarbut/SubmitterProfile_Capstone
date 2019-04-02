#SubmitterRank Functions

## activeUserPull(con, year, sampleSize = None)
Collect list of eligible userids and all of their past submissionids

###Arguments:
con:		psycopg2 connection to rs database
year:		year of last submission (ie. 2018 will only pull users who have submitted since 2018)
sampleSize:	default = None; number of users to pull in random sample of eligible submitters

###Value:
activeUsers:	numpy array of all eligible userids and all of their submissionids
userSet: 	set of unique userids; if sampleSize = None, will be all eligible users; otherwise is random sample of eligible users

## createFormScores(con, discoverTags, usecaseTags, keywordList)
Create topic scores for all relevant forms

Current topic score weighting: 1 pt for each keyword match in form description + 2 pts for each keyword match in form name + 2 pts for each matching Usecase or Discover tag

###Arguments:
con:		psycopg2 connection to rs database
discoverTags:	list of topic-relevant Discover tags
usecaseTags:	list of topic-relevant Usecase tags
keywordList:	string of topic-specific keywords separated by "|"; ie "film|video|animation"

###Value:
formDict:	dictionary with {formid:topicScore}
formSet:	set of formids with any matching keywords or tags

##createUserScores(con, keywordList, userSet)
Create topic scores for all users in userSet

Current topic score weighting: 1 pt for all users + 2 pts for each keyword match in user description

###Arguments:
con:		psycopg2 connection to rs database
keywordList:	string of topic-specific keywords separated by "|"; ie "film|video|animation"
userSet:	set of eligible userids

###Value:
userDf:		pandas dataframe with columns: userid, userScore

## createUserSimilarity(activeUsers, userSet, formSet, formDict)
Calculate topic specific similarity index for each user pair in the userSet. This step currently unable to run on full dataset due to size limitations.

Current similarity index weighting: 1 pt for common submission w/ no topic score + form topic score for common submission w/ topic score

###Arguments:
activeUsers:	numpy array of userids and all of their submissionids
userSet:	set of eligible userids
formSet:	set of formids with any matching keywords or tags
formDict:	dictionary with {formid:topicScore}

###Value:
commonDf:	pandas dataframe with columns: user1, user2, topicScore

##createTransistionMatrix(commonDf, userSet)
Transform dataframe with similarity index for user pairs into transition matrix. Each cell begins as the raw topic specific similarity index between two users, and is transformed into a "similarity proportion" such that each user column sums to one.

###Arguments:
commonDf:	pandas dataframe with columns: user1, user2, topicScore
userSet:	set of eligible userids

###Value:
commonMatrix:	square transition matrix filled with "similarity proportions" for each user pair

##rankIteration(userDf, commonMatrix)
Perform power iteration on transition matrix to create a unique vector of topic specific submitterRanks for all eligible users.

Current parameters: damping factor is set at 0.85; convergence measured at 0.0000001; damping factor weights userDf score vector once at matrix initialization, but not on ea. iteration.

###Arguments:
userDf:		pandas dataframe with columns: userid, userScore
commonMatrix:	square transition matrix filled with "similarity proportions" for each user pair

###Value:
sortedRank:	list of tuples of form: (userid, submitterRank), sorted in descending order of submitterRank

##submitterRank(con, year, discoverTags, usecaseTags, keywordList, sampleSize = None)
Combination of all functions above. Not currently working d/t memory limitations. It's a nice thought though...