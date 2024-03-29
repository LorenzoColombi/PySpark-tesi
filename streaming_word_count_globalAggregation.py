
#Aggregrazione di dati arrivati in una finestra di tempo contente più chunk
#A differenza dello script con lo stesso come senza Aggregation questo mantiene lo stato e aggrega i dati ricevuti in chunk diversi

# Import the necessary classes and create a local SparkContext and StreamingContexts
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# Create Spark Context with two working threads (note, `local[2]`)
sc = SparkContext("local[2]", "StatefulNetworkWordCount")
# Create local StreamingContextwith batch interval of 1 second
ssc = StreamingContext(sc, 1)#1 secondo

# Create checkpoint for local StreamingContext
ssc.checkpoint("checkpoint")

# Define updateFunc: sum of the (key, value) pairs
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

# Create DStream that will connect to the stream of input lines from connection to
localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Calculate running counts
# Line 1: Split lines in to words
# Line 2: count each word in each batch
# Line 3: Run `updateStateByKey` to running count
running_counts = lines.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1))\
    .updateStateByKey(updateFunc)
# Create DStream that will connect to the stream of input lines from connection to localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Print the first ten elements of each RDD generated in this stateful DStream to the console
running_counts.pprint()
# Start the computation
ssc.start()
# Wait for the computation to terminate
ssc.awaitTermination()