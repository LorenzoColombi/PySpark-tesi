#ML + STRUCTURED STREAMING
#Costruzione di un modello ML e predizione in tempo reale sfruttando lo straming strutturato

#Per eseguire: $SPARK_HOME/bin/spark-submit prototipo.py localhost 9999

#Lo streaming da file legge tutti i file .csv nella cartella indicata e si aggiorna ogni volta che un file viene aggiunto (non quando viene modificato lo stesso file)

#IMPORT
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
import json

from pyspark.ml.pipeline import PipelineModel

#Creo al sessione
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

#Disattivo il logging di livello INFO 
spark.sparkContext.setLogLevel('error')

#Recupero dati esportati precedentemente
exportedPath="/home/lorenzo/Documenti/PySpark-tesi/Prototipo/exported"

#Recupero il modello ML salvato
persistedModel = PipelineModel.load(exportedPath)

#Recupero lo schema del DF
with open(exportedPath+"/schema.json") as f:
    new_schema = StructType.fromJson(json.load(f))

#Uso un readstream da file
csvFilePath="/home/lorenzo/Documenti/PySpark-tesi/Prototipo/exported/test/*.csv"

#Lettura dati e creazione DF (INPUT)
csvDF = spark \
    .readStream \
    .schema(new_schema) \
    .csv(csvFilePath)  

#Stampo i dati letti: (OUTPUT)
query = csvDF\
    .writeStream\
    .outputMode('update')\
    .format('console')\
    .start()

#TODO previsione con modello ML e confronto 

#TODO grafico previsione del modello e dato reale


# Await Spark Streaming termination
query.awaitTermination()
