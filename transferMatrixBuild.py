# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 18:34:00 2019

@author: Administrator
"""

import psycopg2
from collections import defaultdict
import numpy as np
import pandas as pd
import _pickle as cpickle
import os

# Creates connection to redshift db using SDM authentication
con=psycopg2.connect(dbname= 'any?', host='', 
                 port= '15440', user= 'any', password= 'any')
cur = con.cursor()

#%%
#select users who have submitted at least twice since 2017 or specifically opted in to marketing services
activeUserq = """with subCount as(
                    select userid, count(distinct submissionid)
                    from submittable_db.submission
                    where 1=1
                    and extract(year from createdon) >=2017
                    group by 1)
                 select distinct s.userid, s.productid 
                 from submittable_db.submission s
                 join subCount sc on s.userid = sc.userid
                 join submittable_db.smmuser u on s.userid = u.userid
                 join submittable_db.product p on s.productid = p.productid
                 join submittable_db.publisher pub on p.publisherid = pub.publisherid
                 where 1=1
                 and (sc.count >1 or u.didconsenttorecommendations is True)
                 and pub.accounttypeid not in (11,16,64)"""

cur.execute(activeUserq)
activeUsers = np.array(cur.fetchall())

activeUserDistinct = list(set(activeUsers[:,0])) 
cpickle.dump(activeUsers, open("activeUsers_8Mar19.pkl", "wb"))   
activeUsers = cpickle.load(open("activeUsers_8Mar19.pkl", "rb"))        
#%%
#create dictionary for ea. active users of all productids submitted to
submitDict = defaultdict(set)

for row in activeUsers:
    user = row[0]
    form = row[1]
    submitDict[user].add(form)

#%%
#create df with user-user pairs and common submission topic scores
#will take waaay too long to do this way for each topic separately
#200B combinations of users plus each submission
#    
#submitDf = pd.DataFrame(columns = ['user1', 'user2', 'topicScore'])
#checked = set()
#for idx, user1 in enumerate(submitDict):
#    print("Checking user", idx, "out of", len(submitDict))
#    for user2 in submitDict:
#        if user2 not in checked:
#            score = 0
#            for form in submitDict[user1]:
#                if form in submitDict[user2]:
#                    if form in filmProductDict:
#                        score += filmProductDict[form]
#                    else:
#                        score += 1
#            pair1 = pd.DataFrame([[user1, user2, score]], columns = ['user1', 'user2', 'topicScore'])
#            link_df = pd.concat([submitDf, pair1], ignore_index = True)
#            pair2 = pd.DataFrame([[user2, user1, score]], columns = ['user1', 'user2', 'topicScore'])
#            link_df = pd.concat([submitDf, pair2], ignore_index = True)
#    checked.add(user1)
#%%
#create dictionary for user pairs and common submissions
#filled RAM to max after 102 users, froze code
#breaking up into groups of 50 users and saving separately
commonDict = {}
checked = set()
counter = 0
for idx, user1 in enumerate(submitDict):
    print("Checking user", idx, "out of", len(submitDict))
    for user2 in submitDict:
        if user2 not in checked:
            common = set()
            for form in submitDict[user1]:
                if form in submitDict[user2]:
                    common.add(form)
            commonDict[(user1,user2)] = common
    checked.add(user1)
#    if idx == 50:
#        break
    if idx !=0 and idx % 50 == 0:
        counter +=1
        cpickle.dump(commonDict, open("commonSubs/commonDict"+str(counter)+".pkl", "wb"), protocol=4)
        commonDict = {}

#%%
#apply topic scores to each user pair based on common submissions
#holy cow, it's going to take a month to get the lists of all user pair common subs
#moving to a sample size for the time being

for d in os.listdir("commonSubs"):
    filename = "commonSubs/"+d
    with open(filename, "rb") as f:
        commonDict = cpickle.load(f, protocol=4)
        
