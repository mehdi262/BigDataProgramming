import sys, math, re, datetime
from uuid import uuid1 as uid 
from pyspark import SparkContext
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql import SQLContext, Row, SparkSession, functions as f, types as t



assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(key_space,table):
    
    nasa_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).load()
    agg_df = nasa_df.groupBy(nasa_df.host).agg(f.count('*').alias('x') \
                                                ,f.sum(nasa_df.bytes).alias('y')) \
                                                .drop('host').drop('id').drop('bytes').drop('datetime').drop('path')
    #print (agg_df.show())
    cal_df = agg_df.withColumn('one',f.lit(1))
    cal_df=cal_df.withColumn('x2',agg_df.x **2)
    cal_df=cal_df.withColumn('y2',agg_df.y **2)
    cal_df=cal_df.withColumn('xy',agg_df.x * agg_df.y)
    
    sum_df = cal_df.groupBy().sum()
    r_nasa_df = sum_df.withColumn('r',(sum_df['sum(one)'] * sum_df['sum(xy)']- sum_df['sum(x)'] * sum_df['sum(y)']) / (f.sqrt(sum_df['sum(one)'] * sum_df['sum(x2)'] - sum_df['sum(x)']**2) * f.sqrt(sum_df['sum(one)']* sum_df['sum(y2)'] - sum_df['sum(y)']**2)))

    final_nasa_df = r_nasa_df.withColumn('r2',r_nasa_df.r ** 2)
    result = final_nasa_df.select(final_nasa_df.r,final_nasa_df.r2).rdd.flatMap(list).collect()

    print('r is equal to : \f',result[0])
    print('r^2 is equal to : \f',result[1])


if __name__ == "__main__":
    print(sys.argv[0])
    key_space = sys.argv[1]
    table = sys.argv[2]    
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Correlation Casandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    sc = spark.sparkContext
    main(key_space,table)