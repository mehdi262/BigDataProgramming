import sys, re, math, datetime, json
from pyspark import SparkConf, SparkContext
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SQLContext, Row, SparkSession, functions as f, types as t

nasa_schema = t.StructType([
    t.StructField('hostname', t.StringType(), False),
    t.StructField('path', t.StringType(), False),
    t.StructField('size', t.FloatType(), False),
    t.StructField('timestamp', t.TimestampType(), False)
])


def make_rdd(line):
    return Row(hostname=line[1],timestamp=datetime.datetime.strptime(line[2], '%d/%b/%Y:%H:%M:%S'),path=line[3], size=float(line[4]))

def main(inputs):
    text = sc.textFile(inputs)
    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    nasa_rdd = text.map(lambda x: linere.split(x)).filter(lambda line: len(line)>4).map(make_rdd)
    nasa_df = sqlContext.createDataFrame(nasa_rdd, nasa_schema)
    agg_df = nasa_df.groupBy(nasa_df.hostname).agg(f.count('*').alias('x') \
                                                ,f.sum(nasa_df.size).alias('y')) \
                                                .drop('hostname')    
    
    cal_df = agg_df.withColumn('one',f.lit(1))
    cal_df=cal_df.withColumn('x2',agg_df.x **2)
    cal_df=cal_df.withColumn('y2',agg_df.y **2)
    cal_df=cal_df.withColumn('xy',agg_df.x * agg_df.y)
    
    cal_df = cal_df.groupBy().sum()
    sum_df = cal_df.select(  cal_df['sum(one)'].alias('n'),cal_df['sum(x)'].alias('sum_x'),cal_df['sum(y)'].alias('sum_y'),cal_df['sum(x2)'].alias('sum_x2') ,cal_df['sum(y2)'].alias('sum_y2'),cal_df['sum(xy)'].alias('sum_xy'))

    r_nasa_df = sum_df.withColumn('r',(sum_df.n  * sum_df.sum_xy- sum_df.sum_x * sum_df.sum_y) / (f.sqrt(sum_df.n * sum_df.sum_x2 - sum_df.sum_x**2) * f.sqrt(sum_df.n * sum_df.sum_y2 - sum_df.sum_y**2)))


    final_nasa_df = r_nasa_df.withColumn('r2',r_nasa_df.r ** 2)
    result = final_nasa_df.select(final_nasa_df.r,final_nasa_df.r2).rdd.flatMap(list).collect()

    print('r is equal to : \f',result[0])
    print('r^2 is equal to : \f',result[1])

if __name__ == '__main__':
    conf = SparkConf().setAppName('Correlate Logs')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    #inputs='/courses/732/nasa-logs-1'
    main(inputs)
