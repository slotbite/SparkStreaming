import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
Crea una conexión con Spark Streaming
El contexto se ejecuta cada 3 segundos y muestra el numero de palabras por cada archivo agregado en la carpeta
"""


def main():
    sc = SparkContext(appName="Streaming")
    ssc = StreamingContext(sc, 3)
    lines = ssc.textFileStream('log/')

    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)

    # df = counts.toDF()
    # df.write.csv('conteos.csv')

    counts.pprint()
    type(counts)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
