import pyspark
from pyspark.sql import SparkSession, functions
spark = SparkSession.builder \
    .master('yarn') \
    .config('spark.driver.memory', '1GB') \
    .config('spark.driver.cores', '1') \
    .config('spark.executor.memory', '1GB') \
    .config('spark.executor.cores', '1') \
    .getOrCreate()
import time
import datetime
from datetime import datetime
from pyspark.sql.functions import col, lit, udf, to_date, to_timestamp, concat_ws
from pyspark.sql.types import StringType
df = spark.read.json('/data/quotations_src/load_dt=20220722/')

def remove_zeros(number):
    while number % 10 == 0:
        number //= 10
    return int(str(number)[0])
remove_zeros_udf = udf(lambda x: remove_zeros(x))

def make_df(x):
    res = df.withColumn(f'{x}_u', col(f'{x}.units')) \
        .withColumn(f'{x}_n', col(f'{x}.nano')) \
        .withColumn(f"{x}_nano_str", col(f"{x}_n").cast(StringType())) \
        .withColumn(f"{x}_units_str", col(f"{x}_u").cast(StringType())) \
        .withColumn(x, concat_ws('.', col(f"{x}_units_str"), col(f"{x}_nano_str")).cast('float')) \
        .drop(f"{x}_u", f"{x}_n", f"{x}_nano_str", f"{x}_units_str")
    return res

columns = ['close', 'high', 'low', 'open']
for i in columns:
    df = make_df(i)

result = df.withColumn('time', to_timestamp(col("time"), 'dd/MM/yyyy HH:mm'))
result.write.mode('overwrite').partitionBy('figi').parquet('/data/quotations_inc/load_dt=20220722')
