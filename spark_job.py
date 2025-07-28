from pyspark.sql import SparkSession

def run_etl():
    spark = SparkSession.builder.appName("ETLTest").getOrCreate()

    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])

    df_filtered = df.filter(df.age > 26)
    df_filtered.show()

    return df_filtered
