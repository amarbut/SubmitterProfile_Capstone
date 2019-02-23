# -*- coding: utf-8 -*-
"""
Created on Sun Jan 13 13:22:23 2019

@author: Anna
"""

import psycopg2
import pandas as pd
import pickle
import os
from sklearn.decomposition import PCA, TruncatedSVD
from scipy.sparse import csr_matrix
from pandas.api.types import CategoricalDtype
import numpy as np
#%%
conn=psycopg2.connect(dbname= 'submittabledata', host='submittable-redshift.cbf41ivkzk1x.us-east-1.redshift.amazonaws.com', 
                     port= '5439', user= '', password= '')

#collect all submitter-opportunity pairs from users who have submitted more
#than 3 times, and opportunities that received more than 3 submissions
sql = """
        with sub_count as(
        select userid, count(submissionid) as num_subs
        from submittable_db.submission
        group by 1),
        form_count as(
        select productid, count(submissionid) as num_subs
        from submittable_db.submission
        group by 1)
        select s.productid, s.userid
        from submittable_db.submission s
        join submittable_db.product p on s.productid = p.productid
        join submittable_db.publisher pub on p.publisherid = pub.publisherid
        join sub_count sc on s.userid = sc.userid
        join form_count fc on s.productid = fc. productid
        where 1=1
        and sc.num_subs >=3
        and fc.num_subs >=3
        and pub.accounttypeid not in (11, 16, 64)
        """
#pull into a 2-column pandas df and save pickle object
submissions = pd.read_sql_query(sql, conn)
pickle.dump(submissions, open("raw_submissions.pkl", "wb"))

#%%
#create dummy variable for "pivot table"
submissions['dummy'] = [1]*len(submissions['userid'])

#create category type for each variable, first converting to strings
submissions['userid'] = submissions['userid'].astype(str)
submissions['productid'] = submissions['productid'].astype(str)

user_c = CategoricalDtype(sorted(submissions['userid'].unique()), ordered = True)
product_c = CategoricalDtype(sorted(submissions['productid'].unique()), ordered = True)

#create "pivot table" for user-opportunity pairs, in sparse form to deal with
#large amount of data and memory allocation errors
#values are 1 for positive user-opportunity matches, and 0 for non-matches
row = submissions['userid'].astype(user_c).cat.codes
col = submissions['productid'].astype(product_c).cat.codes
sparse_try = csr_matrix((submissions['dummy'], (row, col)), shape = (user_c.categories.size, product_c.categories.size))

#save sparse matrix as pickle object
pickle.dump(sparse_try, open("sparse_matrix.pkl", "wb"))
#%%
#use TruncatedSVD for dimension reduction on sparse matrix
#start with 1000 components to determine how many are needed to account for 
#80% of variation in data
svd = TruncatedSVD(n_components = 1000, n_iter = 10)
svd.fit(sparse_try)

#calculate cumulative explained variance of components
explained_var = np.cumsum(svd.explained_variance_ratio_)

#find index of component that is closest to 80% explained variance
min(range(len(explained_var)), key = lambda i: abs(explained_var[i]-0.8))

#use TruncatedSVD with number of components determined above, then apply to 
#sparse matrix to create a reduced matrix
svd2 = TruncatedSVD(n_components = 561, n_iter = 10)
reduced_df = svd2.fit_transform(sparse_try)

#save reduced matrix as pickle object
pickle.dump(reduced_df, open("reduced_matrix.pkl", "wb"))
#%%
#link reduced df back up with userids and assign names to the components

userDict = {}
for i in range(0, len(user_c.categories)):
    user = user_c.categories[i]
    row = list(reduced_df[i,:])
    userDict[user] = row

columns = ["component"+str(i) for i in range(0,561)]
with open("reduced_df.txt", "w") as file:
    headers = ["userid"]
    headers.extend(columns)
    file.write("\t".join(headers))
    for idx, user in enumerate(userDict):
        print(idx, "/", len(userDict)-1)
        cols = "/t".join([str(i) for i in userDict[user]])
        row = "/t".join([user, cols]) + "\n"
        file.write(row)
#data too big to run in R, will continue in python instead
#%%
#create elbow plot to determine optimal number of clusters
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

ssd = []
K = range(1,25)

for k in K:
    print("creating", k, "clusters")
    km = KMeans(n_clusters = k)
    km = km.fit(reduced_df)
    ssd.append(km.inertia_)

#plot sum of squared distances for each number of k
plt.plot(K, ssd, 'bx-')
plt.xlabel('k')
plt.ylabel('Sum_of_squared_distances')
plt.title('Elbow Method For Optimal k')
plt.show()
plt.savefig("form_elbowplot.png")#identify 4, 7, and 12 as potential cluster number

#%%
#create cluster models and link cluster assignments to userid
cluster4 = KMeans(n_clusters = 4).fit(reduced_df)
cluster7 = KMeans(n_clusters = 7).fit(reduced_df)
cluster12 = KMeans(n_clusters = 12).fit(reduced_df)

cluster4_labels = pd.DataFrame({'cluster': cluster4.labels_, 'userid' : user_c.categories})
cluster7_labels = pd.DataFrame({'cluster': cluster7.labels_,'userid' : user_c.categories})
cluster12_labels = pd.DataFrame({'cluster': cluster12.labels_,'userid' : user_c.categories})

cluster4_labels.groupby('cluster').count()
cluster7_labels.groupby('cluster').count()
cluster12_labels.groupby('cluster').count()
