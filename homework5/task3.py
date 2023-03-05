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

df=spark.read \
   .schema(schema) \
   .parquet('/zoomcamp/zoomcamp/fhvhv')

import datetime

df.where((df.pickup_datetime>=datetime.datetime(2021,6,15)) & (df.pickup_datetime<datetime.datetime(2021,6,16))).count()