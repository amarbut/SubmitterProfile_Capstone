# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 14:31:28 2019

@author: Anna
"""

import psycopg2
from collections import defaultdict
import numpy as np
import pandas as pd
import random

def activeUserPull (con, year, sampleSize = None):
    print("Pulling Active Users")
    cur = con.cursor()
    activeUserq = """with subCount as(
                    select userid, count(distinct submissionid)
                    from submittable_db.submission
                    where 1=1
                    and extract(year from createdon) >="""+str(year)+"""
                    group by 1)
                 select distinct s.userid, s.productid 
                 from submittable_db.submission s
                 join subCount sc on s.userid = sc.userid
                 join submittable_db.smmuser u on s.userid = u.userid
                 join submittable_db.product p on s.productid = p.productid
                 join submittable_db.publisher pub on p.publisherid = pub.publisherid
                 where 1=1
                 and (sc.count >1 or u.didconsenttomailinglist is True)
                 and pub.accounttypeid not in (11,16,64)"""

    cur.execute(activeUserq)
    activeUsers = np.array(cur.fetchall())

    activeUserDistinct = list(set(activeUsers[:,0]))
    print("Found", len(activeUserDistinct), "active users")
    #take random sample of designated size
    if sampleSize is not None:
        userSet =  set(random.sample(activeUserDistinct, sampleSize))
        print("Created random list of", sampleSize, "active users")
    else:
        userSet = activeUserDistinct
    
    return activeUsers, userSet

def createFormScores (con, discoverTags, usecaseTags, keywordList):
    cur = con.cursor()
    
    #select forms with matching discover tags
    if discoverTags is not None:
        discoverq = '''select p.productid, t.name
                        from submittable_db.product p
                        join submittable_db.producttag2 pt on p.productid = pt.productid
                        join submittable_db."tag" t on pt.tagid = t.id
                        where t.name in (''' + str(discoverTags).replace("[","").replace("]", "") +")"
    
        cur.execute(discoverq)
        discoverScores = np.array(cur.fetchall())
        discoverSet = set(discoverScores[:,0])
        print("Found", len(discoverSet), "forms with matching discover tags")
    else:
        discoverSet = set()
    
    #select forms with matching usecase tags
    if usecaseTags is not None:
        usecaseq = '''select p.productid, hd.properties__use_case__value as usecase
                        from hubspot.deals hd
                        join submittable_db.product p on hd.properties__admin_id__value__string = p.publisherid
                        where usecase in ('''+ str(usecaseTags).replace("[","").replace("]", "") +")"
    
        cur.execute(usecaseq)
        usecaseScores = np.array(cur.fetchall())
        usecaseSet = set(usecaseScores[:,0])
        print("Found", len(usecaseSet), "forms with matching usecase tags")
    else:
        usecaseSet = set()
    
    #select forms with matching keywords in name or description
    if keywordList is not None:
        keywords = "|".join(keywordList)
        keywordq = "select p.productid, regexp_count(lower(p.description),'"+ keywords+"') as descCount, regexp_count(lower(p.name),'"+ keywords+"""') as nameCount
                        from submittable_db.product p
                        join submittable_db.publisher pub on p.publisherid = pub.publisherid
                        where descCount > 0 or nameCount >0"""
    
        cur.execute(keywordq)
        keyScores = np.array(cur.fetchall())
        keySet = set(keyScores[:,0])
        print("Found", len(keySet), "forms with matching keywords")
    else:
        keySet = set()

    #combine scores for all matching forms
    formSet = keySet.union(usecaseSet).union(discoverSet)
    
    print("Combining scores for", len(formSet), "forms")
    formDict = {}
    for form in formSet:
        if form in keySet:
            row = keyScores[np.where(keyScores[:,0]==form)]
            if row[0,1] is None:
                row[0,1] = 0
            if row[0,2] is None:
                row[0,2] = 0
        if form in usecaseSet:
            #two points for matching usecase
            usecase = 2
        else:
            usecase = 0
        if form in discoverSet:
            #two points for matching discover tag
            discover = 2
        else:
            discover = 0
        #one point for ea. keyword in description
        #two points for ea. keyword in name
        formDict[form]= row[0,1]+ 2*row[0,2]+usecase+discover
    return formDict, formSet

def createUserScores (con, keywordList, userSet):
    cur = con.cursor()
    
    #collect userids based on keywords in names and descriptions
    if keywordList is not None:
        keywords = "|".join(keywordList)
        userKeywordq = "select userid, regexp_count(lower(description), '"+keywords+"""') as numKeys
                        from submittable_db.smmuser
                        where numKeys != 0"""
        
        cur.execute(userKeywordq)
        userScores = np.array(cur.fetchall())
        
        userScoreDict = {}
        for row in userScores:
            userScoreDict[row[0]]=row[1]
    else:
        userScoreDict = {}
    print("Found", len(userScoreDict), "users with matching keywords")

    #create scores for all users in set:
    print("Creating user score index")
    userDict = {}
    for user in userSet:
        if user in userScoreDict:
            userDict[user] = 1 + int(userScoreDict[user])
        else:
            userDict[user] = 1
    
    userDf = pd.DataFrame(columns = ['user','userScore'])
    for user in userDict:
        row = pd.DataFrame([[user, userDict[user]]], columns = ['user', 'userScore'])
        userDf = pd.concat([userDf, row], ignore_index=True)
    userDf = userDf.set_index('user')
    
    return userDf

def createUserSimilarity(activeUsers, userSet, formSet, formDict):
    
    #create dictionary for ea. active users in set of all productids submitted to
    submitDict = defaultdict(list)
    for row in activeUsers:
        if row[0] in userSet:
            user = row[0]
            form = row[1]
            submitDict[user].append(form)

    #create dictionary of all common submissions for user pairs
    commonDict = {}
    checked = set()
    for idx, user1 in enumerate(submitDict):
        print("Creating pairs for user", idx, "out of", len(submitDict))
        for user2 in submitDict:
            if user2 not in checked:
                common = set()
                for form in submitDict[user1]:
                    if form in submitDict[user2]:
                        common.add(form)
                commonDict[(user1,user2)] = common
        checked.add(user1)

    #create product topic score for each user pair and add to df
    commonDf = pd.DataFrame(columns = ['user1', 'user2', 'topicScore'])
    for idx, pair in enumerate(commonDict):
        print("Scoring pair", idx, "out of", len(commonDict))
        user1, user2 = pair
        score = 0
        for form in commonDict[pair]:
            if form in formSet:
                score += 1 + formDict[form]
            else:
                score += 1
        row = pd.DataFrame([[user1, user2, score]], columns = ['user1', 'user2', 'topicScore'])
        commonDf = pd.concat([commonDf, row], ignore_index = True)
        row2 = pd.DataFrame([[user2, user1, score]], columns = ['user1', 'user2', 'topicScore'])
        commonDf = pd.concat([commonDf, row2], ignore_index = True)
    
    commonDf = commonDf.drop_duplicates()
    return commonDf

def createTransitionMatrix(commonDf, userSet):
    #create square matrix with productScores as values  
    print("Forming square matrix of users")
    commonMatrix = pd.crosstab(index = commonDf['user1'], columns = commonDf['user2'], values = commonDf['topicScore'], aggfunc = "sum")

    #add together all pair topic scores for each user
    totalScoreDict = {}
    print("Calculating total topic scores for each user")
    for user in userSet:
        score = sum(commonDf.loc[commonDf['user1']== user]['topicScore'])
        totalScoreDict[user]= score

    #replace raw scores in commonDf with score "probability"
    print("Transforming raw similarity scores to score proportion")
    for user in commonMatrix.index:
        try:
            prob = 1/totalScoreDict[user]
            commonMatrix[(commonMatrix.index == user)] *= prob
        #if no common forms, equal "probability" of linking to any other user
        except ZeroDivisionError:
            commonMatrix[(commonMatrix.index == user)] = [[1/500]*500]
            pass
    
    return commonMatrix

def rankIteration(userDf, commonMatrix):
    
    #re-order matrix rows and user scores to be the same
    print("Re-ordering and transforming matrices")
    row_order = commonMatrix.index.tolist()
    commonMatrix = commonMatrix[row_order]
    userDf = userDf.reindex(row_order)

    #convert commonMatrix to numpy array
    userArray = commonMatrix.values.T.astype(float)
    
    #convert user score vector to np array
    userVector = userDf.iloc[:,0].values.astype(float)
    #replace raw scores with score "probability"
    userVector = userVector/sum(userVector)

    #add teleportation operation
    d = 0.85
    withTeleport = (d * userArray) + ((1-d)*userVector*np.ones([500, 500])).T

    #start rank vector with equal ranks
    print("Performing iterative matrix multiplication")
    r = np.ones(500) / 500
    lastR = r
    # calculate dot-product of transformation matrix (computed by link 
    # probabilities and teleportation operation) and pagerank vector r
    r = withTeleport @ r
    i = 0 #count the number of iterations until convergence
    #break loop once pagerank vector changes less than 0.0001
    while np.linalg.norm(lastR - r) > 0.00001 :
        lastR = r
        r = withTeleport @ r
        i += 1
    print(str(i) + " iterations to convergence.")

    # match userrank vector back up with userids
    rankedUsers = []
    for i in range(0,len(row_order)):
        pair = (row_order[i], r[i]*500)
        rankedUsers.append(pair)
    
    # sort users by topicrank to find highest ranking users
    sortedRank = sorted(rankedUsers, key = lambda tup: tup[1], reverse = True)
    #print top 5 ranked pages
    print(sortedRank[0:5])
    
    return sortedRank

def submitterRank(con, year, sampleSize = None, discoverTags, usecaseTags, keywordList):
    activeUsers, userSet = activeUserPull (con, year, sampleSize = None)
    formDict, formSet = createFormScores (con, discoverTags, usecaseTags, keywordList)
    userDf = createUserScores (con, keywordList, userSet)
    commonDf = createUserSimilarity(activeUsers, userSet, formSet, formDict)
    commonMatrix = createTransitionMatrix(commonDf, userSet)
    sortedRank = rankIteration(userDf, commonMatrix)
    return sortedRank  

