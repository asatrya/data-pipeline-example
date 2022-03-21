import os
from argparse import ArgumentParser
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, to_timestamp, datediff, when, sum, row_number, desc
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


if __name__ == "__main__":

    # parameters
    parser = ArgumentParser()
    parser.add_argument("-e", "--execution-date", dest="execution_date",
                        help="execution date of job")

    args = parser.parse_args()
    execution_date = args.execution_date

    # build spark session
    spark = SparkSession.builder.appName("KoalasPostgresDemo")\
                        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
                        .config("spark.hadoop.fs.s3a.access.key", os.environ.get('AWS_ACCESS_KEY_ID'))\
                        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('AWS_SECRET_ACCESS_KEY'))\
                        .config("spark.hadoop.fs.s3a.path.style.access", True)\
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
                        .config("fs.s3a.connection.ssl.enabled", False)\
                        .config("fs.s3a.signing-algorithm","S3SignerType")\
                        .config("spark.sql.sources.partitionOverwriteMode","dynamic")\
                        .getOrCreate()


    ###################################
    # STEP 1: RAW -> CURATED: cleaning, type assignment, standardization
    ###################################

    # read data
    df = spark.read.parquet("s3a://data/zone_01_raw/data.parquet.gzip")\
                    .where(col('timestamp') <= lit(execution_date))

    # cleaning
    df = df.filter(col('timestamp').isNotNull() & (col('timestamp') != ''))\
            .filter(col('country_code').isNotNull() & (col('country_code') != ''))\
            .filter(col('last_order_ts').isNotNull() & (col('last_order_ts') != ''))\
            .filter(col('total_orders').isNotNull() & (col('total_orders') != ''))\
            .filter(col('voucher_amount').isNotNull())

    # type assignment
    df = df.withColumn('timestamp', col('timestamp').cast(TimestampType()))\
            .withColumn('country_code', col('country_code').cast(StringType()))\
            .withColumn('last_order_ts', col('last_order_ts').cast(TimestampType()))\
            .withColumn('first_order_ts', col('first_order_ts').cast(TimestampType()))\
            .withColumn('total_orders', col('total_orders').cast(IntegerType()))\
            .withColumn('voucher_amount', col('voucher_amount').cast(IntegerType()))

    # standardization
    df = df.withColumn('execution_date', lit(execution_date.replace(':', '')).cast('string').alias('execution_date'))\
            .withColumn('frequent_segment', when(col('total_orders') <= 4, "0-4")\
                                          .when((col('total_orders') > 4) & (col('total_orders') <= 13), "5-13")\
                                          .when((col('total_orders') > 13) & (col('total_orders') <= 37), "13-37")\
                                          .when(col('total_orders') > 37, "37+"))\
            .withColumn('days_since_last_order', datediff(lit(execution_date), col('last_order_ts')))\
            .withColumn('recency_segment', when(col('days_since_last_order') <= 30, "0-30")\
                                         .when((col('days_since_last_order') > 30) & (col('days_since_last_order') <= 60), "30-60")\
                                         .when((col('days_since_last_order') > 60) & (col('days_since_last_order') <= 90), "60-90")\
                                         .when((col('days_since_last_order') > 90) & (col('days_since_last_order') <= 120), "90-120")\
                                         .when((col('days_since_last_order') > 120) & (col('days_since_last_order') <= 180), "120-180")\
                                         .when(col('days_since_last_order') > 180, "180+"))

    # write data to curated
    df.write.mode("overwrite").partitionBy("execution_date").parquet("s3a://data/zone_02_curated/vouchers")


    ###################################
    # STEP 2: CURATED -> PROVISION: aggregation
    ###################################

    # read data
    df_curated = spark.read.option("basePath", "s3a://data/zone_02_curated/vouchers/")\
                            .parquet("s3a://data/zone_02_curated/vouchers/execution_date={}".format(execution_date.replace(':', '')))

    # aggregate frequent_segment
    frequent_window = Window.partitionBy('country_code', 'frequent_segment')\
                            .orderBy(desc('sum_total_orders'))\
                            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_frequent_segment = df_curated.groupBy('execution_date', 'country_code', 'frequent_segment', 'voucher_amount').agg(
                                            sum('total_orders').alias('sum_total_orders'))\
                                    .select(
                                            col('execution_date'),
                                            col('country_code'),
                                            col('frequent_segment'),
                                            col('voucher_amount'),
                                            col('sum_total_orders'),
                                            row_number().over(frequent_window).alias('rank_voucher_used'))\
                                    .where(col('rank_voucher_used') == 1)\
                                    .select(
                                            col('execution_date'),
                                            col('country_code'),
                                            col('frequent_segment'),
                                            col('voucher_amount'))\
                                    .orderBy('country_code', 'frequent_segment', 'rank_voucher_used')

    # write data to provision zone
    df_frequent_segment.write.mode("overwrite")\
                            .partitionBy("execution_date")\
                            .parquet("s3a://data/zone_03_provision/voucher_frequent_most_used")

    # aggregate recency_segment
    recency_window = Window.partitionBy('country_code', 'recency_segment')\
                        .orderBy(desc('sum_total_orders'))\
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_recency_segment = df_curated.groupBy('execution_date', 'country_code', 'recency_segment', 'voucher_amount').agg(
                                            sum('total_orders').alias('sum_total_orders'))\
                                    .select(
                                            col('execution_date'),
                                            col('country_code'),
                                            col('recency_segment'),
                                            col('voucher_amount'),
                                            col('sum_total_orders'),
                                            row_number().over(recency_window).alias('rank_voucher_used'))\
                                    .where(col('rank_voucher_used') == 1)\
                                    .select(
                                            col('execution_date'),
                                            col('country_code'),
                                            col('recency_segment'),
                                            col('voucher_amount'))\
                                    .orderBy('country_code', 'recency_segment', 'rank_voucher_used')

    # write data to provision zone
    df_recency_segment.write.mode("overwrite")\
                            .partitionBy("execution_date")\
                            .parquet("s3a://data/zone_03_provision/voucher_recency_most_used")


    ###################################
    # STEP 3: PROVISION -> CONSUMPTION: expose/publish data
    ###################################

    # read data from provision zone
    df_provision_voucher_frequent = spark.read.option("basePath", "s3a://data/zone_03_provision/voucher_frequent_most_used/")\
                            .parquet("s3a://data/zone_03_provision/voucher_frequent_most_used/execution_date={}".format(execution_date.replace(':', '')))

    df_provision_voucher_recency = spark.read.option("basePath", "s3a://data/zone_03_provision/voucher_recency_most_used/")\
                            .parquet("s3a://data/zone_03_provision/voucher_recency_most_used/execution_date={}".format(execution_date.replace(':', '')))

    # write to mysql table
    df_provision_voucher_frequent.write.format('jdbc').options(
                                                url='jdbc:mysql://mysql/db',
                                                driver='com.mysql.jdbc.Driver',
                                                dbtable='voucher_frequent_most_used',
                                                user='user',
                                                password='password').mode('overwrite').save()

    df_recency_segment.write.format('jdbc').options(
                                                url='jdbc:mysql://mysql/db',
                                                driver='com.mysql.jdbc.Driver',
                                                dbtable='voucher_recency_most_used',
                                                user='user',
                                                password='password').mode('overwrite').save()