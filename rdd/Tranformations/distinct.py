from multiprocessing.reduction import duplicate

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

data = sc.parallelize(["one", 1, 2, 3, 4 , "two", 3, 6, 7, "five", 5, 1, 2 , "three", 10 , "six"])

duplicaterdd = data.distinct()
print(duplicaterdd.collect())