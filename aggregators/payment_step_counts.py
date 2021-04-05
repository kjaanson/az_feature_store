import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.4.0 pyspark-shell'

from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import config

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
sqlc = SQLContext(sc)

readConfig = {
    "Endpoint": config.config['cosmosdb_config']['COSMOSDB_HOST'],
    "Masterkey": config.config['cosmosdb_config']['COSMOSDB_KEY'],
    "Database": "rawdata",
    "Collection": "payments",
    "ReadChangeFeed": "true",
    "ChangeFeedQueryName": "payments_raw",
    "ChangeFeedStartFromTheBeginning": "true",
    "InferStreamSchema": "true",
    "ChangeFeedCheckpointLocation": "/tmp/changefeeds/payments_read"
}


df = spark.readStream \
           .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSourceProvider") \
           .options(**readConfig) \
           .load()

df.createOrReplaceTempView("payments")

sdf_raw = sqlc.sql("""
  SELECT 
      *,
      CAST(from_unixtime(_ts) AS TIMESTAMP) AS timestamp
    FROM payments
""")

#sdf_raw.writeStream.outputMode("append").format("console").start()

sdf_aggregate = sdf_raw \
                  .withWatermark("timestamp","1 day") \
                  .groupBy("step").count()


sdf_aggregate.writeStream.outputMode("update").format("console").start()

writeConfig = {
    "Endpoint": config.config['cosmosdb_config']['COSMOSDB_HOST'],
    "Masterkey": config.config['cosmosdb_config']['COSMOSDB_KEY'],
    "Database": "Surveys",
    "Collection": "Aggr",
    "Upsert": "true",
    "WritingBatchSize": "500",
    "checkpointLocation": "/tmp/checkpointlocation_write1"
}

sdf_aggregate.writeStream \
              .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider") \
              .outputMode("append") \
              .options(**writeConfig) \
              .start()

spark.streams.awaitAnyTermination()

