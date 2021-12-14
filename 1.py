from pyspark.sql import SparkSession
from pyspark.sql.functions import count, lit, col

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

print("Les 10 projets Github pour lesquels il y a eu le plus de commit :")

commits_df.select("repo") \
    .groupBy("repo") \
    .agg(count("repo").alias("count")) \
    .orderBy("count", ascending=False) \
    .show(n=10)