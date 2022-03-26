# -*- coding: utf-8 -*-
"""
Created on Mon May 31 20:08:54 2021

@author: KongM
"""
import pandas as pd
import sklearn
import numpy as np
import matplotlib.pyplot as plt

#used to calculate inertia of k-means analysis
def kmeans_inert(data_df, columns_grouped_list, cluster_num, 
                 clustering_col_name = 'KMeans Cluster'):
    #Change into Numpy Arrow
    data_np = np.asarray(data_df[columns_grouped_list])
    #if array is 1 dimensional, reshape it, as suggested by KMeans for 1-D arrays
    if(len(data_np.shape)==1):
        data_np = data_np.reshape(-1,1)
        
    km = sklearn.cluster.KMeans(n_clusters = cluster_num, init='random',n_init=10, max_iter=300, 
                                tol=1e-04, random_state=0)
    
    data_df[clustering_col_name] = km.fit_predict(data_np)
    sum_sqr_dist_from_centroid = km.inertia_
    
    return {'data_df': data_df, 'inertia': sum_sqr_dist_from_centroid}

#performs k-means analysis on data_df. 
#columns_grouped_list is the list of columns to be clusers
#cluster_max is the number of clusters to test the inertia and perform the elbow method. Will run clusters = 2,3,4,5....
def kmeans_easy(data_df, columns_grouped_list,
                clustering_col_name = 'KMeans Cluster', cluster_max = 10):
    clusters_in_kmeans_list = []
    inertia_list = []
    kmeans_df = []
    
    for x in range(1,cluster_max+1):
        single_output = kmeans_inert(data_df,columns_grouped_list,x,
                     clustering_col_name)
        clusters_in_kmeans_list.append(x)
        inertia_list.append(single_output.get('inertia'))
        #dataframes are objects. have to copy to call different ones
        kmeans_df.append(single_output.get('data_df').copy()) 
    
    plt.plot(clusters_in_kmeans_list, inertia_list, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('Inertia')
    plt.show()    
    return pd.DataFrame({'clusters':clusters_in_kmeans_list, 'inertia':inertia_list,
                        'kmeans_df':kmeans_df})

