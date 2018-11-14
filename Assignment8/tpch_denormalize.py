import sys
from pyspark import  SparkContext
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql import SQLContext, Row, functions as f, types as t

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(key_space,keyspace_to):
 

    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=key_space).load()
    orders_df.createOrReplaceTempView('orders')

    line_item_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=key_space).load()
    line_item_df.createOrReplaceTempView('lineitem')
    
    parts_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=key_space).load()
    parts_df.createOrReplaceTempView('part')

    join_df = spark.sql("SELECT o.*  ,p.name FROM orders o JOIN lineitem l ON (o.orderkey = l.orderkey) JOIN part p ON (p.partkey = l.partkey)")  
    join_df = join_df.groupBy(join_df['orderkey']).agg(f.collect_set(join_df['name']).alias('part_names')).orderBy('orderkey')
    
    result_df=join_df.join(orders_df,['orderkey'],'inner')
    result_df.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace_to).mode("overwrite").option("confirm.truncate",True).save()



if __name__ == "__main__":
    keyspace_from = sys.argv[1]
    keyspace_to = sys.argv[2]
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('TCPH Orders-Parts').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    sc = spark.sparkContext
    main(keyspace_from,keyspace_to)