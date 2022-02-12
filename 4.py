from pyspark.sql.types import *
from pyspark.sql import SparkSession
from utils import display_message
import operator

the_words = {}

def get_words_of_message(message):
    words = message.split(' ')
    for word in words:
        if word in the_words:
            the_words[word] += 1
        else:
            the_words[word] = 1

display_message("Building the stopwords list")
stopwords = []

with open("data/englishST.txt") as file:
  for line in file:
    stopwords.append(line[:-1])

display_message("Building spark session")

spark = SparkSession \
  .builder \
  .appName("Project") \
  .master("local[*]") \
  .getOrCreate()

display_message("Reading CSV file")

schema = "commit STRING, author STRING, date DATE, message STRING"

df = spark.read.option("header", "true").load("data/full.csv", format="csv", schema=schema)
df = df.drop("commit").drop("author").drop("date")

display_message("Filter null messages")

df = df.rdd.filter(lambda x: type(x.message) == str).toDF()

display_message("Putting words in a dictionnary")

iterate = df.rdd.map(lambda x: x.message)
for iteration in iterate.collect():
    get_words_of_message(iteration)

print(df.printSchema())

display_message("Sorting the dictionnary")

the_words = dict(sorted(the_words.items(), key=operator.itemgetter(1), reverse=True))

display_message("Displaying the top words")

count = 0

for key, value in the_words.items():
    if key not in stopwords and key != ' ':
        print(key, ':', value)
        count += 1
        if count >= 10: break