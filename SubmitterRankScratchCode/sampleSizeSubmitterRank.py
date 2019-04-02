# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 13:31:11 2019

@author: Anna
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Feb 24 16:38:53 2019

@author: Anna
"""

import psycopg2
from collections import defaultdict
import numpy as np
import pandas as pd
import random
import pickle
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
                    and extract(year from createdon) >=2018
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
sampleUsers =  set(random.sample(activeUserDistinct, 500)) 
#%%   
pickle.dump(sampleUsers, open("sample500.pkl", "wb"))
sampleUsers = pickle.load(open("sample5000.pkl", "rb"))         
#%%
#create lists of keywords

filmmakerKeys = "film|video|animation|CGI|screenplay|documentary|feature length|screenwriter|shot on mobile"

photographerKeys = "image|photo|portrait"

writerKeys = "manuscript|writing"

fictionKeys = " fiction|story|stories|flash|novel|prose|mystery|sci-fi|science-fiction|speculative|thriller|fantasy|romance|literary"

nonfictionKeys = "nonfiction|essay|prose|journalism|journalist|CNF|pitch"

poetryKeys = "poetry|poem|chapbook|lyric|haiku|stanza"

grantKeys = "grant|foundation|fellowship|letter of intent"

#%%
#Select productids with specifically matched discover labels

discoverFilmTags = ["video", "scripts", "television", "VR/360", "VR", "short-film",
                    "screenwriting", "documentary", "film", "media", "360-Video"]

discoverPhotoTags = ["visual-art", "photography"]

discoverFictionTags = ["fiction","prose","literary","short-story","submishmash","stories",
                       "creative-writing","scripts","science-fiction","monologue","graphic-novel",
                       "short-play","erotica","speculative","anthology","flash","screenwriting","novel"]

discoverNonFictionTags = ["review","prose","nonfiction","memoir","essay","critique",
                          "blog","article","criticism","philosophy","column","journal""journalism","comics"]

discoverPoetryTags = ["chapbook","creative-writing","lyrics","poetry"]

discoverWritingTags = ["book", "publishing", "magazine", "writing", "readings","manuscript"]

discoverGrantTags = ["grant","funding","fellowship"]

#%%

discoverFilmq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverFilmTags).replace("[","").replace("]", "") +")"

cur.execute(discoverFilmq)
discoverFilm = np.array(cur.fetchall())
filmDiscover = set(discoverFilm[:,0])

discoverPhotoq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverPhotoTags).replace("[","").replace("]", "") +")"   

cur.execute(discoverPhotoq)
discoverPhoto = np.array(cur.fetchall())

discoverWritingq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverWritingTags).replace("[","").replace("]", "") +")"

cur.execute(discoverWritingq)
discoverWriting = np.array(cur.fetchall())

discoverFictionq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverFictionTags).replace("[","").replace("]", "") +")"

cur.execute(discoverFictionq)
discoverFiction = np.array(cur.fetchall())

discoverNonFictionq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverNonFictionTags).replace("[","").replace("]", "") +")"

cur.execute(discoverNonFictionq)
discoverNonFiction = np.array(cur.fetchall())
                    
discoverPoetryq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverPoetryTags).replace("[","").replace("]", "") +")"

cur.execute(discoverPoetryq)
discoverPoetry = np.array(cur.fetchall())

discoverGrantq = '''select p.productid, t.name
                    from submittable_db.product p
                    join submittable_db.producttag2 pt on p.productid = pt.productid
                    join submittable_db."tag" t on pt.tagid = t.id
                    where t.name in (''' + str(discoverGrantTags).replace("[","").replace("]", "") +")"

cur.execute(discoverGrantq)
discoverGrant = np.array(cur.fetchall())

#%%
#select productids with topic-specific usecase tags

usecaseFilmq = '''select p.productid, hd.properties__use_case__value as usecase
                    from hubspot.deals hd
                    join submittable_db.product p on hd.properties__admin_id__value__string = p.publisherid
                    where usecase = 'Video/Audio submissions'
                    '''

cur.execute(usecaseFilmq)
usecaseFilm = np.array(cur.fetchall())
filmUsecase = set(usecaseFilm[:,0])

usecaseWritingq = '''select p.productid, hd.properties__use_case__value as usecase
                    from hubspot.deals hd
                    join submittable_db.product p on hd.properties__admin_id__value__string = p.publisherid
                    where usecase in ('Publishing', 'Peer Review', 'Manuscript/Content submissions')
                    '''

cur.execute(usecaseWritingq)
usecaseWriting = np.array(cur.fetchall())
                    
usecaseGrantq = '''select p.productid, hd.properties__use_case__value as usecase
                    from hubspot.deals hd
                    join submittable_db.product p on hd.properties__admin_id__value__string = p.publisherid
                    where usecase in ('Grant applications', 'Grants', 'Fellowship applications', 'Fellowships', 'Corporate Giving')
                    '''

cur.execute(usecaseGrantq)
usecaseGrant = np.array(cur.fetchall())

#%%
#collect productids based on keywords in names and descriptions

productKeywordq = "select p.productid, regexp_count(lower(p.description),'"+ filmmakerKeys+"') as descCount, regexp_count(lower(p.name),'"+ filmmakerKeys+"""') as nameCount
                    from submittable_db.product p
                    join submittable_db.publisher pub on p.publisherid = pub.publisherid
                    where descCount > 0 or nameCount >0"""

cur.execute(productKeywordq)
keyscoresFilm = np.array(cur.fetchall())
filmKeyscores = set(keyscoresFilm[:,0])
#%%
#create topic scores for productids
filmProductSet = filmKeyscores.union(filmUsecase).union(filmDiscover)

filmProductDict = {}
for idx, product in enumerate(filmProductSet):
    print("Scoring", idx)
    if product in filmKeyscores:
        row = keyscoresFilm[np.where(keyscoresFilm[:,0]==product)]
        if row[0,1] is None:
            row[0,1] = 0
        if row[0,2] is None:
            row[0,2] = 0
    else:
        row = np.ones([1,3])*0
    if product in filmUsecase:
        usecase = 4
    else:
        usecase = 0
    if product in filmDiscover:
        discover = 4
    else:
        discover = 0
    filmProductDict[product]= 2*row[0,1]+ 4*row[0,2]+usecase+discover

#%%
#collect userids based on keywords in names and descriptions

userkeywordq = "select userid, regexp_count(lower(description), '"+filmmakerKeys+"""') as numKeys
                from submittable_db.smmuser
                where numKeys != 0"""

cur.execute(userkeywordq)
filmUserScores = np.array(cur.fetchall())

filmUserScoreDict = {}
for row in filmUserScores:
    filmUserScoreDict[row[0]]=4*row[1]

#%%
#create topic scores for all active users
filmUserDict = {}
for idx, user in enumerate(sampleUsers):
    print("Pairing", idx)
    if user in filmUserScoreDict:
        filmUserDict[user] = 1 + int(filmUserScoreDict[user])
    else:
        filmUserDict[user] = 1

filmUserDf = pd.DataFrame(columns = ['user','userScore'])
for user in filmUserDict:
    row = pd.DataFrame([[user, filmUserDict[user]]], columns = ['user', 'userScore'])
    filmUserDf = pd.concat([filmUserDf, row], ignore_index=True)
filmUserDf = filmUserDf.set_index('user')
#%%
#create dictionary for ea. active users in sample of all productids submitted to
submitDict = defaultdict(list)

for row in activeUsers:
    if row[0] in sampleUsers:
        user = row[0]
        form = row[1]
        submitDict[user].append(form)

#%%
#create dictionary of all common submissions for user pairs
commonDict = {}
checked = set()
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
#%%
pickle.dump(commonDict, open("commonDict_sample500.pkl", "wb"))
commonDict = pickle.load(open("commonDict_sample5000.pkl", "rb"))
#%%   
#create product topic score for each user pair and add to df

commonDf = pd.DataFrame(columns = ['user1', 'user2', 'topicScore'])
counter = 1
for idx, pair in enumerate(commonDict):
    print("Scoring pair", idx, "out of", len(commonDict))
    user1, user2 = pair
    score = 0
    for form in commonDict[pair]:
        if form in filmProductSet:
            score += 1 + filmProductDict[form]
        else:
            score += 1
    row = pd.DataFrame([[user1, user2, score]], columns = ['user1', 'user2', 'topicScore'])
    commonDf = pd.concat([commonDf, row], ignore_index = True)
    row2 = pd.DataFrame([[user2, user1, score]], columns = ['user1', 'user2', 'topicScore'])
    commonDf = pd.concat([commonDf, row2], ignore_index = True)
#    if idx % 100000 == 0:
#        commonDf = commonDf.drop_duplicates()
#        with open("sample5000dfs/scoredf_sample5000_"+str(counter)+".pkl", "wb") as pf:
#            pickle.dump(commonDf, pf)
#        commonDf = pd.DataFrame(columns = ['user1', 'user2', 'topicScore'])
#        counter += 1

pickle.dump(commonDf, open("sample500_commonDf.pkl", "wb"))
#%%
#commonDf = pd.DataFrame(columns = ['user1', 'user2', 'topicScore'])
#
#for df in os.listdir("sample5000dfs"):
#    print("Adding", df)
#    filename = "sample5000dfs/"+df
#    smalldf = pickle.load(open(filename, "rb"))
#    commonDf = pd.concat([commonDf, smalldf], ignore_index = True)
#commonDf = commonDf.drop_duplicates()
#  
#%%
#create square matrix with productScores as values
    
commonMatrix = pd.crosstab(index = commonDf['user1'], columns = commonDf['user2'], values = commonDf['topicScore'], aggfunc = "sum")

pickle.dump(commonMatrix, open("sample500_commonMatrix.pkl", "wb"))
commonMatrix = pickle.load(open("sample5000_commonMatrix.pkl", "rb"))
#%%
#add together all pair topic scores for each user
totalScoreDict = {}

for idx, user in enumerate(sampleUsers):
    if user not in totalScoreDict:
        print("User", idx)
        score = sum(commonDf.loc[commonDf['user1']== user]['topicScore'])
        totalScoreDict[user]= score


totalScoreDict = pickle.load(open("sample5000_totalScoreDict.pkl", "rb")) 

#%%
#replace raw scores in commonDf with score "probability"
commonMatrix = commonMatrix.fillna(0)
commonMatrixWgt = 2*commonMatrix
commonMatrixWgt4 = 4*commonMatrix
#%%
totalScoreWgt = {}
for user in commonMatrixWgt.index:
    totalScoreWgt[user] = sum(commonMatrixWgt[user])
#%%

for idx, user in enumerate(commonMatrixWgt.index):
    print("User", idx)
    try:
        prob = 1/totalScoreWgt[user]
        commonMatrixWgt[(commonMatrixWgt.index == user)] *= prob
    #if no common forms, equal "probability" of linking to any other user
    except ZeroDivisionError:
        commonMatrixWgt[(commonMatrixWgt.index == user)] = [[1/5000]*5000]
        pass

#%%
#reorder matrix columns to be in same order as rows
        
row_order = commonMatrixWgt.index.tolist()
commonMatrixWgt = commonMatrixWgt[row_order]

#reorder userScoreVector to be in same order as matrix rows
filmUserDf = filmUserDf.reindex(row_order)

#%%
#convert commonMatrix to numpy array
userArray = commonMatrixWgt.values.T.astype(float)

#convert user score vector to np array
userVector = filmUserDf.iloc[:,0].values.astype(float)

#replace raw scores with score "probability"
userVector = userVector/sum(userVector)

#%%
#add teleportation operation
d = 0.5
withTeleport = (d * userArray) + ((1-d)*userVector*np.ones([5000, 5000])).T

#%%
#start rank vector with equal ranks
r = np.ones(5000) / 5000
lastR = r
# calculate dot-product of transformation matrix (computed by link 
# probabilities and teleportation operation) and pagerank vector r
r = withTeleport @ r
i = 0 #count the number of iterations until convergence
#break loop once pagerank vector changes less than 0.0001
while np.linalg.norm(lastR - r) > 0.0000001 :
    lastR = r
    r = withTeleport @ r
    i += 1
print(str(i) + " iterations to convergence.")
#%%
# match userrank vector back up with userids
rankedUsers = []
for i in range(0,len(row_order)):
    pair = (row_order[i], r[i]*5000)
    rankedUsers.append(pair)

# sort users by topicrank to find highest ranking users
sortedRank = sorted(rankedUsers, key = lambda tup: tup[1], reverse = True)
#print top 5 ranked pages
print(sortedRank[0:5])
#%%
#export to txt for visualization and comparison in R
with open("sortedSampleUserRank_lowd_5000.txt", "w", encoding = "utf-8") as file:
    headers = ["user", "rank"]
    file.write("\t".join(headers)+"\n")
    for pair in sortedRank:
        user, rank = pair
        row = [user, str(rank)]
        file.write("\t".join(row)+"\n")

#%%
#pull list of users from sample that would be selected from keyword-based topic search
        
oldUserListq = """
with film as(
select distinct p.productid
from submittable_db.product p
where 1=1
and extract(year from p.createdate) >2015
and 
p.name similar to '%film%|%short-film%|%filmmaker%|%film-festival%|%video%|%mobile films%|%animation%|%CGI%|%filmmaking%|%films%|%screenplay%'
or p.description similar to '%film%|%short-film%|%filmmaker%|%film-festival%|%video%|%mobile films%|%animation%|%CGI%|%filmmaking%|%films%|%screenplay%'),
users as(
select distinct u.userid
from submittable_db.smmuser u
join submittable_db.submission s on u.userid = s.userid
left join submittable_db.address a on u.addressid = a.addressid
where 1=1
and u.userid in ("""+str(sampleUsers).replace("{","").replace("}", "") +""")
and extract(year from s.createdon)>=2017
and(s.productid in (select productid from film)
or u.description similar to '%film%|%short-film%|%filmmaker%|%film-festival%|%video%|%mobile films%|%animation%|%CGI%|%filmmaking%|%films%|%screenplay%'
or s.coverlettertext similar to '%film%|%short-film%|%filmmaker%|%film-festival%|%video%|%mobile films%|%animation%|%CGI%|%filmmaking%|%films%|%screenplay%'
))
select userid
from users
"""

cur.execute(oldUserListq)
oldUserList = np.array(cur.fetchall())# retrieved 1192 users

with open("sampleOldUserList_500.txt", "w", encoding = "utf-8") as file:
    file.write("userid"+"\n")
    for user in list(oldUserList[:,0]):
        file.write(user+"\n")

#%%
#try again, still with keyword matching but with raw scores for each user

oldScoreListq = """
with film as(
select distinct p.productid, regexp_count(lower(p.description),'"""+ filmmakerKeys+"') as descCount, regexp_count(lower(p.name),'"+ filmmakerKeys+"""') as nameCount
from submittable_db.product p
join submittable_db.publisher pub on p.publisherid = pub.publisherid
where descCount > 0 or nameCount >0),
users as(
select distinct u.userid, regexp_count(lower(u.description), '"""+filmmakerKeys+"""') as numKeys
from submittable_db.smmuser u
join submittable_db.submission s on u.userid = s.userid
where 1=1
and(numKeys != 0
    or s.productid in (select productid from film))
and u.userid in ("""+str(sampleUsers).replace("{","").replace("}", "") +""")),
formScores as(
select distinct u.userid, sum(f.descCount) as totalDesc, sum(f.nameCount) as totalName
from users u
join submittable_db.submission s on u.userid = s.userid
join film f on s.productid = f.productid
group by 1)
select distinct u.userid, u.numKeys, fs.totalDesc, fs.totalName
from users u
join formScores fs on u.userid = fs.userid

"""

cur.execute(oldScoreListq)
oldScoreList = np.array(cur.fetchall())#retrieved 1 users

with open("sampleOldscoreList_500.txt", "w", encoding = "utf-8") as file:
    file.write("\t".join(["userid", "userScore", "formDescScore", "formNameScore"])+"\n")
    for idx, user in enumerate(list(oldScoreList[:,0])):
        userScore = oldScoreList[idx,1]
        formDescScore = oldScoreList[idx,2]
        formNameScore = oldScoreList[idx,3]
        row = "\t".join([user, str(userScore), str(formDescScore), str(formNameScore)])
        file.write(row+"\n")
