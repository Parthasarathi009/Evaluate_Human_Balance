from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

spark = SparkSession.builder.appName("balance_risk_estimator").getORCreate()
spark.sparkContext.setLogLevel("WARN")

kafkaEventsDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load()

# cast the value column in the streaming dataframe as a STRING 
kafkaEventsDf =kafkaEventsDF\
    .selectExpr("cast(key as string) key", "cast(value as string) value")

# parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+

riskSchema = StructType(
    [
        StructField("customer",StringType()),
        StructField("score",StringType()),
        StructField("riskDate",StringType())
    ])
# storing them in a temporary view called CustomerRisk

kafkaEventsDf.withColumn("value",from_json("value",riskSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("CustomerRisk")


# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")
# sink the customerRiskStreamingDF dataframe to the console in append mode

customerRiskStreamingDF.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()\
    .awaitTermination()


#
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 