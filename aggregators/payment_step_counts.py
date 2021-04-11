import string
import random

from pyspark import SQLContext
from pyspark.sql import SparkSession

import config

def aggregate_payments(spark=None):

    if not spark:
        spark = SparkSession.builder.getOrCreate()

    sc = spark.sparkContext
    sqlc = SQLContext(sc)

    r = ''.join(random.choice(string.ascii_lowercase) for i in range(10))

    read_config = {
        "Endpoint": config.config['cosmosdb_config']['COSMOSDB_HOST'],
        "Masterkey": config.config['cosmosdb_config']['COSMOSDB_KEY'],
        "Database": "rawdata",
        "Collection": "payments",
        "ReadChangeFeed": "true",
        "ChangeFeedQueryName": "payments_raw",
        "ChangeFeedStartFromTheBeginning": "true",
        "InferStreamSchema": "true",
        "ChangeFeedCheckpointLocation": "/tmp/checkpoints/payments_read_" + r
    }

    write_config = {
        "Endpoint": config.config['cosmosdb_config']['COSMOSDB_HOST'],
        "Masterkey": config.config['cosmosdb_config']['COSMOSDB_KEY'],
        "Database": "aggregates",
        "Collection": "payments",
        "Upsert": "true",
        "WritingBatchSize": "500",
        "checkpointLocation": "/tmp/checkpoints/payments_write_" + r
    }


    df = spark.readStream \
            .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSourceProvider") \
            .options(**read_config) \
            .load()

    df.createOrReplaceTempView("payments")

    # TODO - Imelik bugi siin.. see sql query ei toimi kui containeris pole ühtegi data välja.
    # Hetkel testis lisan välja enne. Seega on juba midagi seal. Samas see pole tegelt väga ok.
    sdf_raw = sqlc.sql("""
    SELECT 
        *,
        CAST(from_unixtime(_ts) AS TIMESTAMP) AS timestamp
        FROM payments
    """)

    sdf_aggregate = sdf_raw \
                    .withWatermark("timestamp","1 day") \
                    .groupBy("step","type").count()

    stream_writer = sdf_aggregate.writeStream \
                .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider") \
                .outputMode("update") \
                .options(**write_config)

    return stream_writer
