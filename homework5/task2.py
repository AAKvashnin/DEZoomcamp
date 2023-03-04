from pyspark.sql import types

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number',types.StringType(), True)
])


df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('/zoomcamp/zoomcamp/fhv_tripdata/fhvhv_tripdata_2021-06.csv')

df = df.repartition(12)

df.write.parquet('/zoomcamp/zoomcamp/fhvhv')

