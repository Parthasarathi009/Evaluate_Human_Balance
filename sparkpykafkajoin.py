from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# StructType for the Kafka redis-server topic which has all changes made to Redis

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

# StructType for the Customer JSON that comes from Redis

customerjson = StructType(
    [
        StructField("customerName",StringType()),\
        StructField("email",StringType()),\
        StructField("phone",StringType()),\
        StructField("birthDay",StringType())\
    ])

# StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis

riskSchema = StructType(
    [
        StructField("customer",StringType()),
        StructField("score",StringType()),
        StructField("riskDate",StringType())
    ])

# spark application object
spark = SparkSession\
        .builder\
        .appName("sparkpykafkajoin")\
        .getOrCreate()

# set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# Read the streaming dataframe from the Kafka topic redis-server as the source
# specify the option that reads all the events from the topic including those that were published before the spark stream started


kafkaRawStreamingDF = spark\
                        .readStream.format("kafka")\
                        .option("kafka.bootstrap.servers","localhost:9092")\
                        .option("subscribe","redis-server")\
                        .option("startingOffsets","earliest")\
                        .load() 

# cast the value column in the streaming dataframe as a STRING 

kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet

kafkaStreamingDF\
    .withColumn("value",from_json("value",redisMessageSchema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("RedisSortedSet")


# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column

zSetEntriesEncodedStreamingDF = spark.sql("select zSetEntries[0].element as encodedCustomer\
                                            from RedisSortedSet \
                                            where zSetEntries[0].element is not null")

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF\
                                    .withColumn("encodedCustomer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer)\
                                    .cast("string"))

# Decode the encodedCustomer column: which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords

zSetDecodedEntriesStreamingDF\
        .withColumn("encodedCustomer",from_json("encodedCustomer", customerjson))\
        .select(col("encodedCustomer.*"))\
        .createOrReplaceTempView("CustomerRecords")

# select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF

emailAndBirthDayStreamingDF = spark.sql("select email, birthDay \
                                        from CustomerRecords \
                                        where email is not null and birthDay is not null")

# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.withColumn("birthYear", split(col("birthDay"), "-").getItem(0))
emailAndBirthYearStreamingDF = emailAndBirthYearStreamingDF.select(col("email"),col("birthYear"))

# Read the streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before the spark stream started

kafkaRawStreamingDF = spark\
                        .readStream.format("kafka")\
                        .option("kafka.bootstrap.servers","localhost:9092")\
                        .option("subscribe","stedi-events")\
                        .option("startingOffsets","earliest")\
                        .load()  
                                   
# cast the value column in the streaming dataframe as a STRING 

kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

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
#
# storing them in a temporary view called CustomerRisk

kafkaStreamingDF\
    .withColumn("value",from_json("value",riskSchema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("CustomerRisk")

# Select the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer,score from CustomerRisk")

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe

streamsJoin = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr("""email = customer"""))

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 

streamsJoin\
    .selectExpr("cast(customer as string) as key", "to_json(struct(*)) as value")\
    .writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "steditopics")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()