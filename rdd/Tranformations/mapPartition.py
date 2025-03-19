from pyspark import SparkConf, SparkContext
from random import Random
import time

conf = SparkConf().setAppName("map Partition").setMaster("local[*]").set("spark.executor.memory", "6g")
sc = SparkContext(conf=conf)

data = ["Hieu-DE", "Dat-Dev", "Canh-Manager", "Phong-Worker"]
# Hieu-DE: 1500, Dat-Dev: 14000, Canh-Manager: 2000, Phong-Worker: 500
rdd = sc.parallelize(data, 2)
# print(rdd.glom().collect())

# def partition(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
#     return [f"{item}:{rand.randint(0, 1000)}" for item in iterator]
#
# results = rdd.mapPartitions(partition)

results = rdd.mapPartitions(
    lambda index: map(
        lambda l: f"{l}: {Random(int(time.time() * 1000) + Random().randint(0, 1000)).randint(0, 1000)}",
            index
    )
)
print(results.collect())