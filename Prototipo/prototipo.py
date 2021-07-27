#ML + STRUCTURED STREAMING
#Costruzione di un modello ML e predizione in tempo reale sfruttando lo straming strutturato

#CONFIGURAZIONE PATH

#Directory di salvataggio checkpoints
checkPointsPath = "/tmp/PySpark/checkpoints"
#Input modello ML stream da file CSV + schema nel file /schema.json
exportedPath="/home/lorenzo/Documenti/PySpark-tesi/Prototipo/exported"
#Lettura streaming da file (vedi variabile csvFilePath)
csvFilePath="/home/lorenzo/Documenti/PySpark-tesi/Prototipo/exported/test/*.csv"
#Output su file JSON (vedi variabile outputDir)
outputDir = "/home/lorenzo/Documenti/PySpark-tesi/Prototipo/outputStreaming/dataframe.json"

#CONFIGURAZIONE PATH



#Per eseguire: $SPARK_HOME/bin/spark-submit prototipo.py 

#Lo streaming da file legge tutti i file .csv nella cartella indicata e si aggiorna ogni volta che un file viene aggiunto (non quando viene modificato lo stesso file)

#IMPORT
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *

from pyspark.ml.pipeline import PipelineModel

import matplotlib.pyplot as plt

import json

#Creo al sessione
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

#Setto una directory in cui salvare i checkpoint 
spark.conf.set("spark.sql.streaming.checkpointLocation", checkPointsPath)

#Disattivo il logging di livello INFO 
spark.sparkContext.setLogLevel('error')

#Recupero dati esportati precedentemente

#Recupero il modello ML salvato
persistedModel = PipelineModel.load(exportedPath)

#Recupero lo schema del DF
with open(exportedPath+"/schema.json") as f:
    new_schema = StructType.fromJson(json.load(f))

#Uso un readstream da file

#Lettura dati e creazione DF (INPUT)
csvDF = spark \
    .readStream \
    .schema(new_schema) \
    .csv(csvFilePath)  

#Previsione con modello ML
predettoDF = persistedModel.transform(csvDF).select('Elevation', 'prediction')

#Esporto il DF in PANDAS

#Stampo i dati letti (dopo la predizione con ML): (OUTPUT)
query = predettoDF\
    .writeStream\
    .outputMode('update')\
    .format('console')\
    .start()

#Esporto il datafram in un file JSON
queryToFile = predettoDF\
    .writeStream\
    .format('json')\
    .option("path", outputDir)\
    .start()


# Await Spark Streaming termination (2 writeStream - query)
query.awaitTermination()
queryToFile.awaitTermination()
