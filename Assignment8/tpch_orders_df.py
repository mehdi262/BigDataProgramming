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

    Where_condition = tuple([int(x) for x in orderkeys])

    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=key_space).load()
    orders_df.createOrReplaceTempView('orders')

    line_item_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=key_space).load()
    line_item_df.createOrReplaceTempView('lineitem')

    parts_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=key_space).load()
    parts_df.createOrReplaceTempView('part')

    query="""SELECT o.*  ,p.name FROM orders o JOIN lineitem l ON (o.orderkey = l.orderkey) JOIN part p ON (p.partkey = l.partkey) WHERE o.orderkey IN {}""".format(Where_condition)
    join_df = spark.sql(query)  
    join_df = join_df.groupBy(join_df['orderkey'], join_df['totalprice']).agg(f.collect_set(join_df['name']))
    join_df.explain()

    join_rdd = join_df.rdd
    #join_rdd.take(10)
    join_rdd = join_rdd.map(output_line)
    #join_rdd.take(10)
    join_rdd.saveAsTextFile(outdir)


if __name__ == "__main__":
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('TCPH Orders').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    sc = spark.sparkContext
    main(keyspace,outdir,orderkeys)