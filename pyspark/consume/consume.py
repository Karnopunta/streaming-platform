from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, DoubleType
from pyspark.sql.functions import from_json

spark_session = SparkSession\
    .builder\
    .appName("RedpandaSparkStream")\
    .getOrCreate()


stream = spark_session\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:19092")\
    .option("subscribe", "stock_json_topic_spark")\
    .option("startingOffsets", "earliest")\
    .load()

spark_session.sparkContext.setLogLevel('WARN')

stream.printSchema()

json_schema = StructType([
    StructField('event_time', StringType(), True), \
    StructField('ticker', StringType(), True), \
    StructField('price', DoubleType(), True) \
])

# Parse value from binay to string
json_df = stream.selectExpr("cast(value as string) as value")

# Apply Schema to JSON value column and expand the value
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

json_expanded_df.printSchema()

query = json_expanded_df \
    .writeStream \
    .format("console") \
    .start()

query.awaitTermination()
