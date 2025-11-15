from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SimpleDictDataFrame") \
    .getOrCreate()

# Sample data: list of dictionaries
data = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
    {"id": 3, "name": "Charlie", "age": 28},
    {"id": 4, "name": "David", "age": 35},
    {"id": 10, "name": "Jane", "age": 31}
]

# Create DataFrame from dictionary
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()