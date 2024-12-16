from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

silver_dir = "silver"
bio_df = spark.read.parquet(f"{silver_dir}/athlete_bio")
event_df = spark.read.parquet(f"{silver_dir}/athlete_event_results")

bio_df = bio_df.na.drop()
event_df = event_df.na.drop()

event_df = event_df.drop("country_noc")

joined_df = bio_df.join(event_df, on="athlete_id", how="inner")

result_df = (
    joined_df.groupBy("sport", "medal", "sex", "country_noc")
    .agg(avg(col("weight")).alias("avg_weight"), avg(col("height")).alias("avg_height"))
    .withColumn("timestamp", current_timestamp())
)

result_df.write.mode("overwrite").parquet("gold/avg_stats")

result_df.show()
