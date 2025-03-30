from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, desc
from pyspark.sql.types import *
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("Spark SQl with json file") \
    .master("local[*]") \
    .config("spark.executor.memory", "6g") \
    .config("spark.jars", r"C:\Users\PC\PycharmProjects\PythonProject5\Scripts\postgresql-42.7.5.jar") \
    .getOrCreate()

# df = spark.read.text(r"C:\Users\PC\PycharmProjects\PythonProject2\rdd\Data\2015-03-01-17.json")
# df.show()

schemaType = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]), True),
    StructField("repo", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),
        StructField("issue", StructType([
            StructField("url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("labels", ArrayType(StructType([
                StructField("url", StringType(), True),
                StructField("name", StringType(), True),
                StructField("color", StringType(), True),
            ])), True),
            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("assignee", StringType(), True),
            StructField("milestone", StringType(), True),
            StructField("comments", LongType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("body", StringType(), True)

        ]), True),
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True)


])
df = spark.read.schema(schemaType).json(r"C:\Users\PC\PycharmProjects\PythonProject2\rdd\Data\2015-03-01-17.json")
# jsonData.show(truncate=False)
# jsonData.printSchema()

#Select attribute
df = df.select(
    "id",
    "type",
    col("actor.login").alias("actor_login"),  # Đổi tên cột thành actor_login
    col("repo.name").alias("repo_name"),  # Đảm bảo tên là repo_name
    "created_at"
)
# df = df.withColumn("event_date", to_date(col("created_at")))

df = df.na.drop()
df.show(truncate=False)

#Count quantity event by category
event_count = df.groupBy("type").count().orderBy(desc("count"))
event_count.show(truncate=False)

# Top 10 users have most activity
active_users = df.groupBy("actor_login").count().orderBy(desc("count"))
active_users.show(truncate=False)

# Repos have the most traffic
top_repos = df.groupBy("repo_name").count().orderBy(desc("count"))
top_repos.show(truncate=False)

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/github") \
    .option("dbtable", "github_events") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()