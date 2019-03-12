# -*- coding: utf-8 -*-
"""
Created on Sun Feb 24 16:38:53 2019

@author: Anna
"""

import psycopg2
from collections import defaultdict
import numpy as np
import pandas as pd
import pickle

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
filmProductSet = set(filmKeyscores, filmUsecase, filmDiscover)

filmProductDict = {}
for product in filmProductSet:
    if product in filmKeyscores:
        row = keyscoresFilm[np.where(keyscoresFilm[:,0]==product)]
        if row[1] is None:
            row[1] = 0
        if row[2] is None:
            row[2] = 0
    if product in filmUsecase:
        usecase = 2
    else:
        usecase = 0
    if product in filmDiscover:
        discover = 2
    else:
        discover = 0
    filmProductDict[product]= row[1]+ 2*row[2]+usecase+discover


#%%
#collect userids based on keywords in names and descriptions

userkeywordq = "select userid, regexp_count(lower(description), '"+filmmakerKeys+"""') as numKeys
                from submittable_db.smmuser
                where numKeys != 0"""

cur.execute(userkeywordq)
filmUserScores = np.array(cur.fetchall())

filmUserScoreDict = {}
for row in filmUserScores:
    filmUserScoreDict[row[0]]=row[1]

#%%
#create topic scores for all active users
filmUserDict = {}
for user in activeUserDistinct:
    if user in filmUserScoreDict:
        filmUserDict[user] = 1 + int(filmUserScoreDict[user])
    else:
        filmUserDict[user] = 1


    
