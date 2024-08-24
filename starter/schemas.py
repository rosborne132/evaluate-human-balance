from pyspark.sql.types import StructField, StructType, StringType, ArrayType, FloatType

customerRecordsSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)

redisSchema = StructType(
    [
        StructField("key", StringType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", FloatType()),
                    ]
                )
            ),
        ),
    ]
)

stediAppSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType()),
    ]
)
