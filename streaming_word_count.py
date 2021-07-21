 #Semplice esempio di lettura dati (testo) in streaming attraverso una socket

#Prima eseguire in un terminale: nc -lk 9999
#Per lanciare usare: $SPARK_HOME/bin/spark-submit streaming_word_count.py localhost 9999
#Inviare testo da netcat

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

#Spark session


#Spark context
#sc = SparkContext.getOrCreate()
sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel("ERROR") #Altrimenti SPAM e output illeggibile

# Create local StreamingContextwith batch interval of 1 second
ssc = StreamingContext(sc, 1)

# Create Spark Context with two working threads
#sc = SparkContext("local[2]", "NetworkWordCount")

# Create local StreamingContextwith batch interval of 1 second
ssc = StreamingContext(sc, 1)

# Create DStream that will connect to the stream of input lines from connection to localhost:9999
#(ip,porta)
lines = ssc.socketTextStream("localhost", 9999)

# Split lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
#coppie (parola,1)
pairs = words.map(lambda word: (word, 1))
#Lavora su coppie K,V. K è la parola, V =1, li sommo
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()

