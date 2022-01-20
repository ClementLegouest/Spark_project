from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession \
    .builder \
    .appName("Project") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

commits_file = "data/full.csv"

# Readeing data source (csv file)
commits_df = spark.read \
    .format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(commits_file)

print("----------------------------------------")
print("Les 10 personnes qui ont le plus contribué au projet apache/spark ces 24 derniers mois:")
print("----------------------------------------")

# Format de date : Sat Jul 25 10:58:25 2020 -0700

commits_df = commits_df.filter(commits_df.repo == "apache/spark") \
    .select(
    col("author").substr(1, 3).alias("author"),
    col("date"),
    to_date(col("date"), "EEEE MMMM d hh:mm:ss YYYY X").alias("to_date"))

commits_df.show()

commits_df.printSchema()
