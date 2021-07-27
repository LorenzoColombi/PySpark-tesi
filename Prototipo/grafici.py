#Legge JSON e disegna un grafico

#Configurazione PATH
#JSON file path
jsonPath = "/home/lorenzo/Documenti/PySpark-tesi/Prototipo/outputStreaming/dataframe.json/part-00000-bbc7668b-8729-4df5-81fd-9fa2b0c1a901-c000.json"

#IMPORT
import matplotlib 
import matplotlib.pyplot as plt
import json
import pandas as pd
import numpy

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *

#Inizializzo session spark
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

#Carico il DF dal file json
df = forest=spark.read.json(
    jsonPath
)

#Trasformo in Pandas DF
pandasDF = df.toPandas()
print(pandasDF)

#Disegno il grafico sovrapponendo le due linee
lines=pandasDF.plot.line()
plt.show()

