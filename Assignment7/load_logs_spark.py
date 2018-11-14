import sys, math, re, datetime
from uuid import uuid1 as uid 
from pyspark import SparkContext
from pyspark.sql import SparkSession

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_words_map(line):
    linex = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    w = linex.split(line)
    if(len(w) >= 4):
        yield ({'host':w[1], 'datetime': datetime.datetime.strptime(w[2], '%d/%b/%Y:%H:%M:%S'), 'path': w[3], 'bytes': float(w[4]),'id':str(uid())})

def main(inputs,key_space,table):
    text = sc.textFile(inputs) 
    p_num=200
    text = text.repartition(p_num)
    result = text.flatMap(get_words_map).toDF()
    result.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).save()

if __name__ == "__main__":
    inputs = sys.argv[1]
    key_space = sys.argv[2]
    table = sys.argv[3]
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Nasa Logs Casandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    sc = spark.sparkContext
    main(inputs,key_space,table)