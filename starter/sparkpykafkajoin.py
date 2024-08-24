from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, split, expr
from helpers import createTopic
from schemas import stediAppSchema, redisSchema, customerRecordsSchema

# Create a spark application object
spark = SparkSession.builder.appName("stedi-app").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redisRawStreamingDF = createTopic(spark, "redis-server")

# Cast the value column in the streaming dataframe as a STRING
redisStreamingDF = redisRawStreamingDF.selectExpr("CAST(value AS string) AS value")

# Parse the single column "value" with a json object in it, like this:
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
redisStreamingDF.withColumn("value", from_json("value", redisSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisSortedSet")

# Execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and
# create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to
# select the nth element of an array in a sql column
zSetEntriesEncodedStreamingDF = spark.sql(
    "SELECT key, zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet"
)

# Take the encodedCustomer column which is base64 encoded at first like this:
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
# +--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string")
)

# Parse the JSON in the Customer record and store in a temporary view called CustomerRecords
zSetDecodedEntriesStreamingDF.withColumn(
    "customer", from_json("customer", customerRecordsSchema)
).select(col("customer.*")).createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe
# called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql(
    "SELECT * FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL"
)

# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.withColumn(
    "birthYear",
    split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"),
)


# Using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
stediAppRawStreamingDF = createTopic(spark, "stedi-events")

# Cast the value column in the streaming dataframe as a STRING
stediAppStreamingDF = stediAppRawStreamingDF.selectExpr("CAST(value AS string) value")

# Parse the JSON from the single column "value" with a json object in it, like this:
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
stediAppStreamingDF.withColumn("value", from_json("value", stediAppSchema)).select(
    col("value.*")
).createOrReplaceTempView("CustomerRisk")

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary view,
# creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# Join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
joinedCustomerRiskAndBirthDF = customerRiskStreamingDF.join(
    emailAndBirthYearStreamingDF, expr("""customer = email""")
)

# Sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
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
joinedCustomerRiskAndBirthDF.selectExpr(
    "CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").option(
    "topic", "stedi-graph"
).option(
    "checkpointLocation", "/tmp/checkPointKafka"
).start().awaitTermination()
