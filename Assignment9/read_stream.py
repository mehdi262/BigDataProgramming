
import sys, math, re, datetime, uuid
from kafka import KafkaConsumer
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row, SparkSession, functions as f, types as t

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+




def main(topic):

    messages = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
            .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    values=values.withColumn('x', f.split(values['value'],' ').getItem(0).cast('integer'))
    values=values.withColumn('y', f.split(values['value'],' ').getItem(1).cast('integer'))
    values=values.withColumn("xy", values['x'] * values['y'])
    values=values.withColumn("x2", values['x'] * values['x'])
    values=values.withColumn('c',f.lit(1))

    sum_df= values.groupBy().sum()
    

    
    sum_df=sum_df.withColumn('slope',(sum_df['sum(xy)']-(sum_df['sum(x)']* sum_df['sum(y)'])/sum_df['sum(c)'])/(sum_df['sum(x2)']-(sum_df['sum(x)']**2)/sum_df['sum(c)']))
    sum_df=sum_df.withColumn('intercept',(sum_df['sum(y)']/sum_df['sum(c)'])-(sum_df['slope'] * (sum_df['sum(x)']/sum_df['sum(c)'])))   
    sum_df=sum_df.drop('sum(x)').drop('sum(y)').drop('sum(xy)').drop('sum(x2)').drop('sum(c)')

    stream = sum_df.writeStream.format('console').outputMode('update').start()     
    stream.awaitTermination(600)
    
    
if __name__ == "__main__":
    topic = sys.argv[1]
    spark= SparkSession.builder.appName('Read Stream').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    main(topic)


