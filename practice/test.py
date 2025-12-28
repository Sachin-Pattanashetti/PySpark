from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
import time

spark = SparkSession.builder \
    .appName("Slow Spark Job - Spark UI Demo") \
    .master("local[*]") \
    .getOrCreate()

print("Spark UI URL:", spark.sparkContext.uiWebUrl)

# --------------------------------------------------
# STEP 1: Read large CSV file
# --------------------------------------------------
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("C:/Git files/My git files/PySpark/files/employee_data.csv")

# --------------------------------------------------
# STEP 2: Define a SLOW UDF
# (simulates heavy business logic)
# --------------------------------------------------
def slow_salary_calc(salary):
    time.sleep(0.0005)   # intentional delay
    return salary + 1000

slow_udf = udf(slow_salary_calc, IntegerType())

# --------------------------------------------------
# STEP 3: Apply UDF (row-by-row execution)
# --------------------------------------------------
df2 = df.withColumn("updated_salary", slow_udf(col("salary")))

# --------------------------------------------------
# STEP 4: Wide Transformation (Shuffle)
# --------------------------------------------------
result_df = df2.groupBy("dept") \
    .sum("updated_salary")

# --------------------------------------------------
# STEP 5: Action (Triggers execution)
# --------------------------------------------------
result_df.show()

spark.stop()
