import sys
from pyspark import  SparkContext
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql import SQLContext, Row, functions as f, types as t

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def output_line(line):
    namestr = ', '.join(sorted(list(line[2])))
    return 'Order #%d $%.2f: %s' % (line[0], line[1], namestr)

def main(key_space,outdir,orderkeys):
 
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=key_space).load()


    orders_df=orders_df.filter(orders_df['orderkey'].isin(orderkeys))
    result_df =orders_df.groupBy(['orderkey','totalprice']).agg(f.first('part_names').alias('part_names') ).orderBy('orderkey')
    

    result_df.explain()

    result_rdd = result_df.rdd
    result_rdd.take(10)
    result_rdd = result_rdd.map(output_line)
    result_rdd.take(10)
    result_rdd.saveAsTextFile(outdir)


if __name__ == "__main__":
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('TCPH Order Denorms').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    sc = spark.sparkContext
    main(keyspace,outdir,orderkeys)