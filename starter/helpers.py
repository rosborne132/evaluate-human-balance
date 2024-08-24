def createTopic(spark, topicName):
    """
    Creates a streaming DataFrame by subscribing to a specified Kafka topic.

    This function initializes a streaming read from a Kafka source, specifically targeting
    a given topic. It configures the Kafka server details, the topic to subscribe to, and
    the starting offset from which to begin consumption. The function returns a streaming DataFrame
    that represents the data being continuously read from the specified Kafka topic.

    Parameters:
    - spark (SparkSession): The SparkSession object used to access Spark functionalities.
    - topicName (str): The name of the Kafka topic to subscribe to.

    Returns:
    pyspark.sql.dataframe.DataFrame: A streaming DataFrame connected to the specified Kafka topic.

    Example:
    >>> spark = SparkSession.builder.appName("KafkaStreamingApp").getOrCreate()
    >>> kafkaDF = createTopic(spark, "myTopic")
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:19092")
        .option("subscribe", topicName)
        .option("startingOffsets", "earliest")
        .load()
    )
