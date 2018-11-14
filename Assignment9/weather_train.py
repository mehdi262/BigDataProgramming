import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+
from pyspark.sql.functions import dayofyear
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(inputs, outputs):
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    query1="""SELECT *,  dayofyear( date ) AS day
                FROM __THIS__ """
    # TODO: create a pipeline to predict Latitude, Longtitude, Elevation point -> tmax
   
    query="""SELECT today.station as station ,
                    today.latitude as latitude,
                    today.longitude as longitude,
                    today.elevation as elevation, 
                    dayofyear( today.date ) AS day, 
                    today.tmax, 
                    yesterday.tmax AS yesterday_tmax 
                FROM __THIS__ as today 
                    INNER JOIN __THIS__ as yesterday 
                        ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"""
    sqlTrans = SQLTransformer(statement=query)
          
    lle_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation","day","tmax","yesterday_tmax" ], outputCol="features")
    tmax_indexer = StringIndexer(inputCol="station", outputCol="label", handleInvalid='error')
    regressor = GBTRegressor(featuresCol = 'features', labelCol = 'tmax', maxIter = 100)

    pipeline = Pipeline(stages=[sqlTrans,lle_assembler, tmax_indexer, regressor])
    model = pipeline.fit(train)
    
    # TODO: create an evaluator and score the validation 
    predictions = model.transform(validation)
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    model.write().overwrite().save(outputs)

    print('r2 =', r2)
    print('rmse =', rmse)



    
if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs= sys.argv[2]
    main(inputs,outputs)

