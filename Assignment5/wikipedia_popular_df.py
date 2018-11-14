import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as f , types as t
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext
    
wiki_schema = t.StructType([ # commented-out fields won't be read
    t.StructField('language', t.StringType(), True),
    t.StructField('title', t.StringType(), True),
    t.StructField('views', t.IntegerType(), True),
    t.StructField('size', t.StringType(), True),
])

# add more functions as necessary

def reverse(str):
    return str[::-1]

@f.udf(returnType=t.StringType())
def path_to_hour(path):
    rp=reverse(path)
    endPart=reverse(rp.split('/')[0])
    hour=endPart[11:22]
    return hour


def main(inputs, output):
    # main logic starts here    
    df=spark.read.csv(inputs,sep=' ', schema=wiki_schema ).withColumn('hour', path_to_hour(f.input_file_name()))
    wiki=df.select('hour','language','title','views','size')
    new_wiki=wiki.filter(wiki.language=="en").filter( wiki.title.startswith('Main_Page')==False).filter( wiki.title.contains('Special:')==False).cache()

    max_df = new_wiki.groupBy(new_wiki.hour).agg(f.max(new_wiki.views).alias('max'))
    join_df=new_wiki.join(max_df,(max_df.hour == new_wiki.hour) & (new_wiki.views == max_df.max)).drop_duplicates()
    final=join_df.select(new_wiki.hour, new_wiki.title, new_wiki.views).sort(new_wiki.hour, new_wiki.title)
    final.explain()
    final.write.json(output,mode='overwrite')
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)