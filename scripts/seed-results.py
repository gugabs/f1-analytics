from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("CSV to MongoDB") \
  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
  .config("spark.mongodb.output.uri", "mongodb://mongo-db:27017/f1.results") \
  .getOrCreate()

csv_constructors_path = "data/constructors.csv"
csv_results_path = "data/results.csv"

constructors_df = spark.read.csv(csv_constructors_path, header=True, inferSchema=True)
constructors_df = constructors_df.select("constructorId", "name")

results_df = spark.read.csv(csv_results_path, header=True, inferSchema=True)
results_df = results_df.select("resultId", "constructorId", "position")

merged_df = results_df.join(constructors_df, on="constructorId", how="inner")
merged_df = merged_df.select("resultId", "name", "position")

merged_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()

spark.stop()