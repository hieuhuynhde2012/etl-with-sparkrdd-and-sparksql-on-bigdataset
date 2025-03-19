from pyspark import  SparkConf, SparkContext

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
rdd = sc.parallelize(numbers)

squareRdd = rdd.map(lambda x: x * x)
filterRdd = rdd.filter(lambda x: x >= 5)
flatMapRdd = rdd.flatMap(lambda x: [[x, x* 2]])

print(squareRdd.collect())
print(filterRdd.collect())
print(flatMapRdd.collect())