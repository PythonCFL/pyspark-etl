from pyspark.sql import SparkSession
from transformations import clean_taxis, avg_trip_by_barrio

spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_clean_taxis_removes_nulls_and_invalids():
    data = [(1, 2.5), (None, 3.0), (2, 0)]
    df = spark.createDataFrame(data, ["passenger_count", "trip_distance"])
    result = clean_taxis(df).collect()
    assert all(r["trip_distance"] > 0 for r in result)

def test_avg_trip_by_barrio():
    data = [("Centro", 2.0), ("Centro", 4.0), ("Norte", 5.0)]
    df = spark.createDataFrame(data, ["barrio", "trip_distance"])
    result = avg_trip_by_barrio(df).collect()
    centro = [r for r in result if r["barrio"] == "Centro"][0]
    assert abs(centro["avg_distance"] - 3.0) < 0.01
