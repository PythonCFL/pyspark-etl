from pyspark.sql import SparkSession
from transformations import clean_taxis, join_with_barrios, avg_trip_by_barrio

def main():
    spark = SparkSession.builder \
        .appName("PySpark ETL") \
        .getOrCreate()

    # Ingesta de datos
    taxis = spark.read.csv("data/taxis.csv", header=True, inferSchema=True)
    barrios = spark.read.csv("data/barrios.csv", header=True, inferSchema=True)

    # Transformaciones
    taxis_clean = clean_taxis(taxis)
    df_join = join_with_barrios(taxis_clean, barrios)
    df_avg = avg_trip_by_barrio(df_join)

    # Mostrar resultados
    print("==== Muestra de viajes limpios ====")
    taxis_clean.show(5)

    print("==== Join con barrios ====")
    df_join.show(5)

    print("==== Distancia media por barrio ====")
    df_avg.show()

    # Escritura de salida particionada en Parquet
    df_join.write.mode("overwrite").partitionBy("barrio").parquet("output/taxis_clean")

    spark.stop()

if __name__ == "__main__":
    main()