
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1,2,3,4,5, 6, 7, 8])
rdd2 = sc.parallelize([1,2,3,4,5, 6, 7, 8, 10, 13, 15, 16])

rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())