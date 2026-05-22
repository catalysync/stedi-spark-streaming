from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, split
from pyspark.sql.types import (
    ArrayType, BooleanType, StringType, StructField, StructType,
)


redisServerSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("Ch", BooleanType()),
    StructField("Incr", BooleanType()),
    StructField("zSetEntries", ArrayType(StructType([
        StructField("element", StringType()),
        StructField("Score", StringType()),
    ]))),
])


customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email",        StringType()),
    StructField("phone",        StringType()),
    StructField("birthDay",     StringType()),
])


spark = SparkSession.builder.appName("STEDI-redis-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


redisServerRawStreamingDF = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:19092")
        .option("subscribe", "redis-server")
        .option("startingOffsets", "earliest")
        .load()
)


redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("CAST(value AS STRING) AS value")


redisServerStreamingDF \
    .withColumn("value", from_json("value", redisServerSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("RedisSortedSet")


encodedCustomerStreamingDF = spark.sql(
    "SELECT zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet"
)


decodedCustomerStreamingDF = encodedCustomerStreamingDF.withColumn(
    "customer", unbase64(col("encodedCustomer")).cast("string")
)


decodedCustomerStreamingDF \
    .withColumn("customer", from_json("customer", customerSchema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")


emailAndBirthDayStreamingDF = spark.sql(
    "SELECT email, birthDay FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL"
)


emailAndBirthYearStreamingDF = (
    emailAndBirthDayStreamingDF
        .withColumn("birthYear", split(col("birthDay"), "-").getItem(0))
        .select("email", "birthYear")
)


emailAndBirthYearStreamingDF.writeStream \
    .outputMode("append").format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
