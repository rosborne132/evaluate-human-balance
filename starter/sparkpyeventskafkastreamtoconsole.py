from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from helpers import createTopic
from schemas import stediAppSchema

spark = SparkSession.builder.appName("stedi-events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
stediAppRawStreamingDF = createTopic(spark, "stedi-events")

# Cast the value column in the streaming dataframe as a STRING
stediAppStreamingDF = stediAppRawStreamingDF.selectExpr(
    "CAST(key AS string) AS key", "CAST(value AS string) AS value"
)

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

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe
# called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# Sink the customerRiskStreamingDF dataframe to the console in append mode
#
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
customerRiskStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()

# Run the python script by running the command from the terminal:
# ./submit-event-kafkastreaming.sh
# Verify the data looks correct
