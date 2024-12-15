from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, mean, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaProducer
from configs import kafka_config, spark_mysql_config
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import current_timestamp, mean, window
import json

event_result_json_schema = StructType([
    StructField("athlete_id", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("event", StringType(), True)
])

spark = SparkSession.builder \
    .appName("AthleteDataProcessing") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Етап 1. зчитую дані з MySQL таблиці olympic_dataset.athlete_bio
data_athlete_bio = spark.read \
    .format("jdbc") \
    .option("url", spark_mysql_config["ol_db_url"]) \
    .option("dbtable", "athlete_bio") \
    .option("user", spark_mysql_config["user"]) \
    .option("password", spark_mysql_config["password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Етап 2. Відфільтрувати дані
filtered_athlete_bio = data_athlete_bio \
    .filter(~(col("height").isNull() | col("weight").isNull() | isnan(col("height")) | isnan(col("weight")))) \
    .withColumn("height", col("height").cast(FloatType())) \
    .withColumn("weight", col("weight").cast(FloatType()))
filtered_athlete_bio = filtered_athlete_bio.withColumnRenamed('sex', 'gender')


# Етап 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results.
data_event_results = spark.read \
    .format("jdbc") \
    .option("url", spark_mysql_config["ol_db_url"]) \
    .option("dbtable", "athlete_event_results") \
    .option("user", spark_mysql_config["user"]) \
    .option("password", spark_mysql_config["password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
)
for row in data_event_results.collect():
    row_dict = row.asDict()
    producer.send("athlete_event_results", value=row_dict)
    print(f"Sent data: {row_dict}")

raw_event_results_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "athlete_event_results") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_event_results = raw_event_results_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_result_json_schema).alias("data")) \
    .select("data.*")

parsed_event_results_with_watermark = parsed_event_results \
    .withColumn("event_time_a", current_timestamp()) \
    .withWatermark("event_time_a", "2 minutes")

cleaned_filtered_athlete_bio_with_watermark = filtered_athlete_bio \
    .withColumn("event_time", current_timestamp()) \
    .withWatermark("event_time", "2 minutes")

# Етап 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
joined_data = parsed_event_results_with_watermark.join(
    cleaned_filtered_athlete_bio_with_watermark, 
    on="athlete_id", 
    how="inner"
)

# Етап 2. Відфільтрувати дані - про всяк випадок
joined_data = joined_data.filter(
    col("sport").isNotNull() & 
    col("medal").isNotNull() & 
    col("gender").isNotNull() & 
    col("country_noc").isNotNull()
)

# Етап 5. Агрегування
aggregated_data = joined_data \
    .groupBy("sport", "medal", "gender", "country_noc") \
    .agg(
        mean("height").alias("average_height"),
        mean("weight").alias("average_weight")
    )

# Етап 6. Зробіть стрим даних (за допомогою функції forEachBatch
def foreach_batch_function(batch_df, epoch_id):
    print(f"Processing batch {epoch_id} with {batch_df.count()} records")
    batch_df.selectExpr("to_json(struct(*)) AS value").write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_athlete_data") \
        .save()
    batch_df.write \
        .format("jdbc") \
        .option("url", spark_mysql_config["neo_db_url"]) \
        .option("dbtable", "vp_aggregated_athlete_data") \
        .option("user", spark_mysql_config["user"]) \
        .option("password", spark_mysql_config["password"]) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

aggregated_data.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
