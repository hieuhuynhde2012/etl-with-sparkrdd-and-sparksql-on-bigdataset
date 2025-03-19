from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap").setMaster("local[*]").set("spark.executor.memory", "6g")
sc = SparkContext(conf=conf)

data = (
    {"id": 1, "name": "Hieu"},
    {"id": 2, "name": "Dat"},
    {"id": 3, "name": "Canh"},
)

rdd1 = sc.parallelize(data)
rdd2 = sc.parallelize([1, 2, 3])
rdd3 = rdd1.union(rdd2)
print(rdd3.collect())