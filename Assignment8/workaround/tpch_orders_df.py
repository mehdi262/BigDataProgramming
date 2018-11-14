# Personal Note, if any!
from pyspark import SparkContext
import sys, math
from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
def output_line(line):
    namestr = ', '.join(sorted(list(line[2])))
    return 'Order #%d $%.2f: %s' % (line[0], line[1], namestr)

def main(keyspace, outdir, orderkeys):
    orderkeys = tuple([int(x) for x in orderkeys])
    df_orders = spark.read.format(
                            "org.apache.spark.sql.cassandra"
                            ).options(
                                      table = 'orders',
                                      keyspace = keyspace
                                      ).load()
    df_orders.createOrReplaceTempView("orders")
    df_part = spark.read.format(
                            "org.apache.spark.sql.cassandra"
                            ).options(
                                      table = 'part',
                                      keyspace = keyspace
                                      ).load()
    df_part.createOrReplaceTempView("part")
    df_lineitem = spark.read.format(
                            "org.apache.spark.sql.cassandra"
                            ).options(
                                      table = 'lineitem',
                                      keyspace = keyspace
                                      ).load()
    df_lineitem.createOrReplaceTempView("lineitem")
    query = """SELECT o.*, p.name
        FROM orders o
        JOIN lineitem l ON l.orderkey = o.orderkey
        JOIN part p ON p.partkey = l.partkey
        WHERE o.orderkey IN {}""".format(orderkeys)
    df = spark.sql(query)
    df_final = df.groupBy(
                          ['orderkey','totalprice']
                          ).agg(
                                functions.collect_set('name').alias('names')
                                ).orderBy('orderkey')
    df_final.explain()
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
