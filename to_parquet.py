import pyspark
from pyspark.sql import SparkSession, functions
import time
import datetime
from datetime import datetime
from pyspark.sql.functions import col, lit, udf, to_date, to_timestamp, concat_ws
from pyspark.sql.types import StringType

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('yarn') \
        .config('spark.driver.memory', '1GB') \
        .config('spark.driver.cores', '1') \
        .config('spark.executor.memory', '1GB') \
        .config('spark.executor.cores', '1') \
        .getOrCreate()

    df = spark.read.json('/data/quotations_src/load_dt=20220722/')

    def without_units_nano(f_df, x):
        return f_df.withColumn(x, concat_ws('.', col(f'{x}.units').cast(StringType()), col(f'{x}.nano').cast(StringType())).cast('float'))

    columns = ['close', 'high', 'low', 'open']
    for i in columns:
        df = without_units_nano(df, i)

    time_format = 'dd/MM/yyyy HH:mm'
    df.withColumn('time', to_timestamp(col("time"), time_format)).write.mode('overwrite').partitionBy('figi').parquet('/data/quotations_inc/load_dt=20220722')

    result.write.mode('overwrite').partitionBy('figi').parquet('/data/quotations_inc/load_dt=20220722')
