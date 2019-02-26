# -*- coding: utf-8 -*-
"""
Created on Sun Feb 24 16:38:53 2019

@author: Anna
"""

import psycopg2
from collections import defaultdict
import numpy as np

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
                 select distinct s.userid 
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

activeUserSet = set(activeUsers[:,0])                 
#%%
#create lists of keywords

filmmakerKeys = ["film", "short-film", "filmmaker", "film-festival", "video",
                 "mobile films", "animation", "CGI", "filmmaking", "films", "screenplay"]

photographerKeys = ["photography", "photograph", "photographer", "image",
                    "photographs", "photo", "photo-contest", "photography-contest"]

fictionKeys = ["fiction", "short-story", "story", "stories", "flash", "novel",
               "novelist", "prose", "short-stories"]

nonfictionKeys = []

poetryKeys = []

grantKeys = []

#%%
#Select productids with specifically matched labels

