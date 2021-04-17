from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
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

# TO-DO: create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic

customerjson = StructType([\
                           StructField("customerName",StringType()),\
                           StructField("email",StringType()),\
                           StructField("phone",StringType()),\
                           StructField("birthDay",StringType())\
                          ])
 

# TO-DO: create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic

riskSchema = StructType([\
                           StructField("customer",StringType()),\
                           StructField("score",StringType()),\
                           StructField("riskDate",StringType())\
                        ])
#TO-DO: create a spark application object
spark = SparkSession.builder.appName("sparkpykafkajoin").getOrCreate()

#TO-DO: set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

kafkaRawStreamingDF = spark.readStream.format("kafka").\
option("kafka.bootstrap.servers","localhost:9092").\
option("subscribe","redis-server")\
.option("startingOffsets","earliest").load() 

# TO-DO: cast the value column in the streaming dataframe as a STRING 
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# storing them in a temporary view called RedisSortedSet
kafkaStreamingDF.withColumn("value",from_json("value",redisMessageSchema)).\
select(col('value.*')).\
createOrReplaceTempView("RedisSortedSet")

# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

zSetEntriesEncodedStreamingDF = spark.sql("select zSetEntries[0].element as encodedCustomer from RedisSortedSet where zSetEntries[0].element is not null")

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("encodedCustomer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
zSetDecodedEntriesStreamingDF.withColumn("encodedCustomer",from_json("encodedCustomer", customerjson))\
.select(col("encodedCustomer.*"))\
.createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql("select email, birthDay from CustomerRecords where email is not null and birthDay is not null")


# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.withColumn("birthYear", split(col("birthDay"), "-").getItem(0))

emailAndBirthYearStreamingDF = emailAndBirthYearStreamingDF.select(col("email"),col("birthYear"))

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

kafkaRawStreamingDF = spark.readStream.format("kafka").\
option("kafka.bootstrap.servers","localhost:9092").\
option("subscribe","stedi-events")\
.option("startingOffsets","earliest").load()  
                                   
# TO-DO: cast the value column in the streaming dataframe as a STRING 
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# storing them in a temporary view called CustomerRisk
kafkaStreamingDF.withColumn("value",from_json("value",riskSchema))\
.select(col('value.*'))\
.createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer,score from CustomerRisk")


# TO-DO: join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe

streamsJoin = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr("""email = customer"""))

# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 

streamsJoin.selectExpr("cast(customer as string) as key", "to_json(struct(*)) as value")\
.writeStream.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("topic", "steditopics")\
.option("checkpointLocation","/tmp/kafkacheckpoint")\
.start()\
.awaitTermination()