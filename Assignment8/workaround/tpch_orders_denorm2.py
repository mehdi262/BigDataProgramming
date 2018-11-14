# Personal Note, if any!
from pyspark import SparkContext
import sys, math
from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def output_line(line):
    namestr = ', '.join(sorted(list(line[2])))
    return 'Order #%d $%.2f: %s' % (line[0], line[1], namestr)

def main(keyspace, outdir, orderkeys):

    df = spark.read.format(
                           "org.apache.spark.sql.cassandra"
                           ).options(
                                     table = 'orders_parts',
                                     keyspace = keyspace
                                     ).load()
                                     
    df = df.filter(df['orderkey'].isin(orderkeys))
    
    df_final = df.groupBy(
                          ['orderkey','totalprice']
                          ).agg(
                                functions.first('part_names').alias('part_names')
                                ).orderBy('orderkey')
    #df_final.explain()
    rdd1 = df_final.rdd
    rdd2 = rdd1.map(output_line)
    rdd2.saveAsTextFile(outdir)

if __name__ == '__main__':
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra TPCH'
                                         ).config(
                                                  'spark.cassandra.connection.host', ','.join(cluster_seeds)
                                                  ).config(
                                                           'spark.dynamicAllocation.maxExecutors', 16
                                                           ).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)
