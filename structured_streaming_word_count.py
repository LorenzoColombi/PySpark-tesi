#Streaming strutturato e analisi continua
#Ultimo esempio del libro

#Per eseguire: $SPARK_HOME/bin/spark-submit structured_streaming_word_count.py localhost 9999

# Import the necessary classes and create a local SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

#Creo al sessione
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel('error')

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark\
    .readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 9999)\
    .load()

# Split the lines into words
words = lines.select(
    explode(
    split(lines.value, ' ')
    ).alias('word')
)

# Generate running word count
wordCounts = words.groupBy('word').count()

# Start running the query that prints the running counts to the console
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

# Await Spark Streaming termination
query.awaitTermination()