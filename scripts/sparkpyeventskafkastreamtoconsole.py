from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, FloatType


customerRiskSchema = StructType([
    StructField("customer", StringType()),
    StructField("score",    FloatType()),
    StructField("riskDate", StringType()),
])


spark = SparkSession.builder.appName("STEDI-events-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


stediEventsRawStreamingDF = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:19092")
        .option("subscribe", "stedi-events")
        .option("startingOffsets", "earliest")
        .load()
)


stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr("CAST(value AS STRING) AS value")


stediEventsStreamingDF \
    .withColumn("value", from_json("value", customerRiskSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")


customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")


customerRiskStreamingDF.writeStream \
    .outputMode("append").format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
