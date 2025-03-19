from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("join").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

data = sc.parallelize([(110, 50.54), (126, 12.21), (130, 45.9), (100, 99.0, (140, 47.7), (110, 15.09))])
data1 = sc.parallelize([(110, "a"), (120, "b"), (130, "e"), (100, "a"), (140, "e")])
join = data.join(data1).sortByKey()
print(join.collect())

for result in join.collect():
    print(result)