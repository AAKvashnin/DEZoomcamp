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

zones_schema=types.StructType([
        types.StructField('LocationID', types.IntegerType(), True),
        types.StructField('Borough', types.StringType(), True),
        types.StructField('Zone', types.StringType(), True),
        types.StructField('service_zone', types.StringType(), True)
        ])

zones=spark.read.option("header","true").schema(zones_schema).csv('/zoomcamp/zoomcamp/fhv_tripdata/taxi_zone_lookup.csv')

df.registerTempTable('fhv_data')
zones.registerTempTable('zones')

spark.sql("""
          SELECT zones.Zone, COUNT(1) as cnt
          FROM fhv_data JOIN zones ON fhv_data.PULocationID=zones.LocationID
          GROUP BY zones.Zone
          ORDER BY cnt DESC
          """).show()
