from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\'"]', "", str(text))


clean_text_udf = udf(clean_text, StringType())

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    df = spark.read.parquet(f"bronze/{table}")
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))
    
    df = df.dropDuplicates()
    df.write.mode("overwrite").parquet(f"silver/{table}")

    df.show()
