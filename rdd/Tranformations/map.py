from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("map").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

rdd = sc.textFile(r"C:\Users\PC\PycharmProjects\PythonProject2\rdd\Data\text.txt")
# print(rdd.collect())

allCapRdd = rdd.map(lambda line: line.upper())
print(allCapRdd.collect())

for line in allCapRdd.collect():
    print(line)