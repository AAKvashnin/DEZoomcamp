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

from pyspark.sql import functions as F

df=df.withColumn('hours_between',(F.unix_timestamp(F.col('dropoff_datetime'))-F.unix_timestamp(F.col('pickup_datetime')))/3600)

df.agg(F.max(F.col('hours_between'))).show()