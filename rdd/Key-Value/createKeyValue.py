from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("createKeyValue").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

rdd = sc.parallelize(["hieu", "dat", "canh", "phong"])
#hieu: 4, dat:3, canh:4, phong: 5
keyvalue = rdd.map(lambda x: (len(x), x))
# print(keyvalue.collect())
#
# for pair in keyvalue.collect():
#     print(pair)

groupByKey = keyvalue.groupByKey()
for i in groupByKey.collect():
    print(i)

for key, value in groupByKey.collect():
    print(key, list(value))