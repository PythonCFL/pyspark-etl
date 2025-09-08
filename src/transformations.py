from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg

def clean_taxis(df: DataFrame) -> DataFrame:
    """
    Limpieza básica de taxis:
    - Eliminar registros nulos en passenger_count y trip_distance
    - Filtrar viajes con distancia mayor a 0
    """
    return df.dropna(subset=["passenger_count", "trip_distance"]) \
             .filter(col("trip_distance") > 0)

def join_with_barrios(taxis: DataFrame, barrios: DataFrame) -> DataFrame:
    """
    Join entre dataset de taxis y barrios por barrio_id.
    """
    return taxis.join(barrios, on="barrio_id", how="left")

def avg_trip_by_barrio(df: DataFrame) -> DataFrame:
    """
    Calcula la distancia media de viajes por barrio.
    """
    return df.groupBy("barrio").agg(avg("trip_distance").alias("avg_distance"))