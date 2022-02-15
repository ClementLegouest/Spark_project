from pyspark.sql import SparkSession
from pyspark.sql.functions import count, lit, col
from time import sleep
from utils import display_message

display_message("Creating SparkSession")

spark = SparkSession \
    .builder \
    .appName("Project") \
    .master("local[*]") \
    .getOrCreate()

commits_file = "data/full.csv"

display_message("Reading data")

commits_df = spark.read \
    .format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)

display_message("Les 10 projets Github pour lesquels il y a eu le plus de commit :")

commits_df.select("repo") \
    .groupBy("repo") \
    .agg(count("repo").alias("count")) \
    .orderBy("count", ascending=False) \
    .show(n=10)

sleep(1000000000)