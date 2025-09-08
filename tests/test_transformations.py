from pyspark.sql import SparkSession
from src.transformations import clean_taxis, avg_trip_by_barrio, join_with_barrios

# Creamos SparkSession para los tests
spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_clean_taxis_removes_nulls_and_invalids():
    """
    Verifica que clean_taxis elimina nulos y distancias <= 0
    """
    data = [
        (1, 2.5, 1),   # válido
        (None, 3.0, 1), # nulo en passenger_count → fuera
        (2, 0.0, 2)    # distancia 0 → fuera
    ]
    df = spark.createDataFrame(data, ["passenger_count", "trip_distance", "barrio_id"])
    result = clean_taxis(df).collect()

    assert all(r["trip_distance"] > 0 for r in result)
    assert all(r["passenger_count"] is not None for r in result)

def test_avg_trip_by_barrio():
    """
    Verifica que avg_trip_by_barrio calcula correctamente la media
    """
    data = [
        ("Centro", 2.0),
        ("Centro", 4.0),
        ("Norte", 5.0)
    ]
    df = spark.createDataFrame(data, ["barrio", "trip_distance"])
    result = avg_trip_by_barrio(df).collect()

    # Extraemos resultado para "Centro"
    centro = [r for r in result if r["barrio"] == "Centro"][0]
    assert abs(centro["avg_distance"] - 3.0) < 0.01

def test_join_with_barrios():
    """
    Verifica que join_with_barrios une correctamente taxis con barrios
    """
    taxis_data = [
        (1, 2.5, 1),
        (2, 4.0, 2),
    ]
    barrios_data = [
        (1, "Centro"),
        (2, "Norte"),
    ]

    taxis = spark.createDataFrame(taxis_data, ["passenger_count", "trip_distance", "barrio_id"])
    barrios = spark.createDataFrame(barrios_data, ["barrio_id", "barrio"])

    df_join = join_with_barrios(taxis, barrios).collect()

    # Verificamos que el join añadió el campo "barrio"
    barrios_joined = [r["barrio"] for r in df_join]
    assert "Centro" in barrios_joined
    assert "Norte" in barrios_joined
