from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from time import sleep

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

display_message("Les 10 personnes qui ont le plus contribué au projet apache/spark ces 24 derniers mois:")

# Managing the date to compare with
date = datetime.now()
earlier_date = date - relativedelta(months=24)

print("aujourd'hui : ", date)
print("Il y a 24 mois : ", earlier_date)

# Format de date : Sat Jul 25 10:58:25 2020 -0700

commits_df = commits_df.filter(commits_df.repo == "apache/spark") \
    .select(
    col("author").alias("author"),
    col("date"),
    to_date(col("date"), "EEEE MMMM d hh:mm:ss YYYY X").alias("to_date"),
    col("repo")) \
    .filter(commits_df("date").gt(earlier_date)) \
    .show(30, False)

# commits_df.show(30, False)

commits_df.printSchema()

sleep(1000000000)