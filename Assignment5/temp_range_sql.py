import sys
from pyspark.sql import SparkSession, functions as f, types, SQLContext

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('Weater Sql ETL').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

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
    w = spark.read.csv(inputs, schema=observation_schema).cache()
    w.createOrReplaceTempView("weather")


    w_max = spark.sql("SELECT date, station , value as tmax FROM weather WHERE qflag is NULL AND observation = 'TMAX' ")
    w_max.createOrReplaceTempView("w_max")

    w_min = spark.sql("SELECT date, station , value as tmin FROM weather WHERE qflag is NULL AND observation = 'TMIN' ")
    w_min.createOrReplaceTempView("w_min")

    w_range = spark.sql("SELECT w_max.date as date, w_max.station as station, CAST((w_max.tmax - w_min.tmin)/10 AS DOUBLE) as range FROM w_max JOIN w_min ON w_max.date = w_min.date AND w_max.station = w_min.station ORDER BY w_max.date").cache()
    w_range.createOrReplaceTempView("w_range")

    w_max_range = spark.sql("Select w_range.date, MAX(w_range.range) as max_range from w_range GROUP BY w_range.date")
    w_max_range.createOrReplaceTempView("w_max_range")

    w_final = spark.sql("SELECT w_range.date as date, w_range.station as station, w_range.range FROM w_range JOIN w_max_range ON w_range.date = w_max_range.date AND w_range.range = w_max_range.max_range ORDER BY w_range.date")
    #w_final.show(100)
    w_final.write.csv(output, mode='overwrite')#, compression='gzip')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
