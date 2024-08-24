def createTopic(spark, topicName):
    return spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", "kafka:19092")\
        .option("subscribe", topicName)\
        .option("startingOffsets", "earliest")\
        .load()
