from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from time import sleep

spark = SparkSession \
    .builder \
    .appName("Project") \
    .master("local[*]") \
    .getOrCreate()

commits_file = "data/full.csv"

commits_df = spark.read \
    .format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)

display_message("La personne qui a le plus contribué au projet apache/spark :")

commits_df.filter(commits_df.repo == "apache/spark") \
    .select("author") \
    .groupBy("author") \
    .agg(count("author").alias("count")) \
    .orderBy("count", ascending=False) \
    .show(n=1)

count(1000000000)