from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('TPCH Benchmark for Python').getOrCreate()


decrease = udf(lambda x, y: x * (1 - y), FloatType())
increase = udf(lambda x, y: x * (1 + y), FloatType())


lineitem = spark.read.parquet("data/lineitem.parquet")
lineitem.show()
lineitem.filter(col("l_shipdate") <= "1998-09-02") \
.groupBy(col("l_returnflag"), col("l_linestatus")) \
.agg(F.sum(col("l_quantity")).alias("sum_qty"),
        F.sum(col("l_extendedprice")).alias("sum_base_price"),
        F.sum(decrease(col("l_extendedprice"), col("l_discount"))).alias("sum_disc_price"),
        F.sum(increase(decrease(col("l_extendedprice"), col("l_discount")), col("l_tax"))).alias("sum_charge"),
        F.avg(col("l_quantity")).alias("avg_qty"),
        F.avg(col("l_extendedprice")).alias("avg_price"),
        F.avg(col("l_discount")).alias("avg_disc"),
        F.count(col("l_quantity")).alias("count_order")) \
.sort(col("l_returnflag"), col("l_linestatus")) \
.show()