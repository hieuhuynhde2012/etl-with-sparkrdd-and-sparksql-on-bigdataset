from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("createKeyValue").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

rdd = sc.parallelize([("hieu", 500.2), ("dat", 600.3), ("canh", 900.4),("hieu", 100.2), ("phong", 400.5), ("dat", 50.3),("canh", 100.4) ])

billSalary = rdd.reduceByKey(lambda key, value: key + value)
print(billSalary.collect())
