import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Window

spark = SparkSession.builder.appName('Whather ETL').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])
def main(inputs, output):
    
    
    w = spark.read.csv(inputs, schema=observation_schema)
    weather= w.filter(w.qflag.isNull()).cache()
    w_max = weather.filter(weather.observation == 'TMAX')
    w_max=w_max.withColumnRenamed('value', 'tmax')
    w_min = weather.filter(weather.observation == 'TMIN')
    w_min=w_min.withColumnRenamed('value', 'tmin')    
    w_range = w_max.join(w_min,(w_max.date == w_min.date) & (w_max.station == w_min.station)).select(w_max.date,w_max.station,((w_max.tmax - w_min.tmin)/10).alias('range')).cache()
    w_max_range = w_range.withColumn('max_range',functions.max('range').over(Window.partitionBy('date')) ) 
    w_final = w_max_range.filter(functions.col('range') == functions.col('max_range') ).drop('max_range').orderBy('date')
    w_final.write.csv(output, mode='overwrite')#, compression='gzip')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)