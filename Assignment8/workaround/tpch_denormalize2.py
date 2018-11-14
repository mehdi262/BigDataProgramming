# Personal Note, if any!
from pyspark import SparkContext
import sys, math
from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def output_line(line):
    namestr = ', '.join(sorted(list(line[2])))
    return 'Order #%d $%.2f: %s' % (line[0], line[1], namestr)
 
def main(keyspace, keyspace2):

    df_orders = spark.read.format(
                            "org.apache.spark.sql.cassandra"
                            ).options(
                                      table = 'orders',
                                      keyspace = keyspace
                                      ).load()
    
    df_part = spark.read.format(
                            "org.apache.spark.sql.cassandra"
                            ).options(
                                      table = 'part',
                                      keyspace = keyspace
                                      ).load()
    
    df_lineitem = spark.read.format(
                            "org.apache.spark.sql.cassandra"
                            ).options(
                                      table = 'lineitem',
                                      keyspace = keyspace
                                      ).load()

    cond1 = ['partkey']
    df_f1 = df_part.join(df_lineitem, cond1, 'inner')
    cond2 = ['orderkey']
    df_f2 = df_f1.join(df_orders, cond2, 'inner')
    
    df_final = df_f2.groupBy(
                             ['orderkey']#,'totalprice']
                          ).agg(
                                functions.collect_set('name').alias('part_names')
                                ).orderBy('orderkey')
    print("first \n\n\n\n")
    print(df_final.show(10))

    df_final = df_final.join(df_orders, cond2, 'inner')
    print("Second \n\n\n\n")
    print(df_final.show(10))
    df_final.write.format(
                          "org.apache.spark.sql.cassandra"
                          ).options(
                                    keyspace = keyspace2,
                                    table = 'orders_parts'
                                    ).mode(
                                           "overwrite"
                                           ).option(
                                                    "confirm.truncate",True
                                                    ).save()

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
    keyspace2 = sys.argv[2]
    main(keyspace, keyspace2)
