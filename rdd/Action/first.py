from pyspark import SparkConf, SparkContext

sc = SparkContext("local", "DE-ETL-102")

data = (
    {"id": 1, "name": "Hieu"},
    {"id": 2, "name": "Dat"},
    {"id": 3, "name": "Canh"},
)

rdd = sc.parallelize(data)

print(rdd.first())
