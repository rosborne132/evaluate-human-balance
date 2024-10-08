from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, split
from helpers import createTopic
from schemas import redisSchema, customerRecordsSchema

# Create a spark application object
spark = SparkSession.builder.appName("customer-redis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redisRawStreamingDF = createTopic(spark, "redis-server")

# Cast the value column in the streaming dataframe as a STRING
redisStreamingDF = redisRawStreamingDF.selectExpr("cast(value AS string) AS value")

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

# From the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"),
)

# Sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
#
# The output should look like this:
# +--------------------+-----
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----
emailAndBirthYearStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()

# Run the python script by running the command from the terminal:
# ./submit-redis-kafka-streaming.sh
# Verify the data looks correct
