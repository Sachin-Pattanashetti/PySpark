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
    {"id": 5, "name": "Eva", "age": 22},
    {"id": 6, "name": "Frank", "age": 40},
    {"id": 7, "name": "Grace", "age": 27},
    {"id": 8, "name": "Hannah", "age": 32},
    {"id": 9, "name": "Ian", "age": 29},
    {"id": 10, "name": "Jane", "age": 31}
]

# Create DataFrame from dictionary
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()