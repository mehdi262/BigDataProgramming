import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark import SparkConf, SparkContext, Row
from pyspark.sql import SQLContext, functions as f, types as t 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
 
graph_schema = StructType([
    StructField('source', StringType(), False),
    StructField('destination', StringType(), False)
])
known_paths_schema = StructType([
    StructField('node', StringType(), False),
    StructField('source', StringType(), False),
    StructField('distance', IntegerType(), False)
])
sim_schema = StructType([
    StructField('node', StringType(), False)
])
def map_path(ab):
    a,b = ab
    return (a,(b.split()))

def main(inputs,output,source,destination):
    text = sc.textFile(inputs)
    if(source == destination):
        print('source and destination are same')
        sys.exit()
    graph_rdd = text.map(lambda line: line.split(':'))\
                .filter(lambda sec: len(sec)>1)\
                .map(map_path ).flatMapValues(lambda x: x)
    graph_df = sqlContext.createDataFrame(graph_rdd, graph_schema).cache() 

    known_paths_df = sqlContext.createDataFrame(sc.parallelize([[source,'-', 0]]),known_paths_schema)

    next_node_df = sqlContext.createDataFrame(sc.parallelize([[source]]),sim_schema)

    for i in range(6):
        print ('iter- d:',i)
        known_paths_df.show()
        known_paths_df.write.save(output + '/iter' + str(i), format='json')
        adjacent_df = graph_df.join(next_node_df, graph_df.source == next_node_df.node) \
                                        .select(graph_df.source,graph_df.destination).cache()      
        new_paths_df = known_paths_df.join(adjacent_df, known_paths_df.node == adjacent_df.source) \
                                        .select( adjacent_df.destination.alias('node'),known_paths_df.node.alias('source'),(known_paths_df.distance+1).alias('distance')).cache()
        known_paths_df = known_paths_df.unionAll(new_paths_df).dropDuplicates().cache() 
        shortest_paths_df = known_paths_df.groupBy(known_paths_df.node).agg(f.min(known_paths_df.distance).alias('minimum_dist'))
        shortest_paths_df = shortest_paths_df.withColumnRenamed('node','minimum_node')
        known_paths_df =  shortest_paths_df.join(known_paths_df,(shortest_paths_df.minimum_node == known_paths_df.node) & (shortest_paths_df.minimum_dist == known_paths_df.distance)) \
                                                .select(known_paths_df.node,known_paths_df.source,known_paths_df.distance)

        next_node_df = adjacent_df.select('destination').withColumnRenamed('destination','node')
        if next_node_df.filter(next_node_df.node == destination).count() > 0 :
            print('Found')
            break
    print('\n\n\n\n\nfinal paths:')
    known_paths_df.show(10)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Shortest Path')
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)

    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs,output,source,destination)