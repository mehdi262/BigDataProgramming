import sys, math, re, datetime
#from uuid import uuid1 as uid 
#from pyspark import SparkContext
#from pyspark.sql import SparkSession
from cassandra.cluster import Cluster



assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(key_space,table):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    cluster = Cluster(cluster_seeds)

    session = cluster.connect(key_space)
    some_host='rpgopher.aist.go.jp'
    rows = session.execute("SELECT path, bytes FROM nasalogs WHERE host =%s;" ,[some_host])
    for row in rows:
        print(row)
    
if __name__ == "__main__":
    key_space = sys.argv[1]
    table = sys.argv[2]    

    main(key_space,table)