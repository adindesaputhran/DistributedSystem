import time, pickle
import warnings
from geopy.distance import great_circle

warnings.simplefilter('ignore')

from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
# from sklearn.cluster import OPTICS
# from geopy.distance import great_circle
from shapely.geometry import MultiPoint

from sklearn import metrics
from sklearn.metrics import pairwise_distances

import socket
from typing import Any, Union

from pandas import DataFrame
from pandas.io.parsers import TextFileReader
import pandas as pd
#from sys import kmeans_local

def get_centermost_point(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)

def load_data():
    address_url = "dataset/hi.csv"
    address = pd.read_csv(address_url)
    df = address
    df = df.drop(['NUMBER', 'STREET', 'UNIT', 'CITY', 'DISTRICT', 'REGION', 'POSTCODE', 'ID', 'HASH'], axis=1)
    return df[['LAT', 'LON']]

def kmeans_local(df):
    start_time = time.time()
    model = KMeans(n_clusters=20)

    Kmeans2 = model.fit_predict(df)
    print("Calinski_harabaz_score: ", metrics.calinski_harabaz_score(df, Kmeans2))
    df['Cluster'] = Kmeans2
    cluster_count = df.groupby('Cluster').count().sort_values(by=['LAT'], ascending=False)[:10]
    rep = pd.DataFrame(data={'LON': df['LON'], 'LAT': df['LAT'], 'Count': Kmeans2})
    print(" --- KMEANS --- ", time.time() - start_time, " seconds ---")
    rep.to_csv('output/kmeans_out.csv')
    """pl.figure('K-Means Clustering')
    pl.scatter(df['LON'], df['LAT'], c=model.labels_)
    pl.xlabel('Longitude')
    pl.ylabel('Latitude')
    pl.title('K-Means')
    pl.savefig('graphs/kmeans.png')"""
    return rep

# next create a socket object



s = socket.socket()
print("Socket successfully created")

# reserve a port on your computer in our
# case it is 12345 but it can be anything
port = 65001

# Next bind to the port
# we have not typed any ip in the ip field
# instead we have inputted an empty string
# this makes the server listen to requests
# coming from other computers on the network
s.bind(('127.0.0.1', port))
print("socket binded to %s" % (port))
#op = kmeans_local(sys.load_data())
# put the socket into listening mode
s.listen(5)
print("socket is listening")
# op = kmeans_local(load_data())
# a forever loop until we interrupt it or
# an error occurs
while True:
    # Establish connection with client.
	c, addr = s.accept()
	print('Got connection from', addr)
	opt = int(c.recv(4096))
	op = []
	if opt is 1:
		op = kmeans_local(load_data())
	c.send(pickle.dumps(op))
	c.close()
    # Close the connection with the client
