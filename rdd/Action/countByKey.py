from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("join").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

data = sc.parallelize([(110, 50.54), (126, 12.21), (130, 45.9), (100, 99.0), (140, 47.7), (110, 15.09)])
print(dict(data.countByKey()))