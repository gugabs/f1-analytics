from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

spark = SparkSession.builder \
  .appName("Seed lap times") \
  .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
  .config("spark.cassandra.connection.host", "cassandra-db") \
  .config("spark.cassandra.connection.port", "9042") \
  .config("spark.cassandra.output.consistency.level", "ONE") \
  .getOrCreate()

csv_circuits_path = "data/circuits.csv"
csv_races_path = "data/races.csv"
csv_lap_times_path = "data/lap_times.csv"

circuits_df = spark.read.csv(csv_circuits_path, header=True, inferSchema=True)
circuits_df = circuits_df.select("circuitId", "name")

races_df = spark.read.csv(csv_races_path, header=True, inferSchema=True)
races_df = races_df.select("raceId", "year", "circuitId")

lap_times_df = spark.read.csv(csv_lap_times_path, header=True, inferSchema=True)
lap_times_df = lap_times_df.select("raceId", "driverId", "lap", "time", "milliseconds")

merged_df = races_df.join(circuits_df, on="circuitId", how="inner")
merged_df = merged_df.join(lap_times_df, on="raceId", how="inner")
merged_df = merged_df.select("raceId", "year", "name", "driverId", "lap", "time", "milliseconds")

cluster = Cluster(['cassandra-db'])
session = cluster.connect()

keyspace_name = "f1"
replication_options = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
}

create_keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {str(replication_options)}"
session.execute(create_keyspace_query)

create_table_query = """
  CREATE TABLE IF NOT EXISTS f1.lap_times (
    "raceId" INT,
    "year" INT,
    "name" TEXT,
    "driverId" INT,
    "lap" INT,
    "time" TEXT,
    "milliseconds" INT,

    PRIMARY KEY (("name"), "year", "raceId", "driverId", "lap")
  )
"""

session.execute(create_table_query)

cluster.shutdown()

merged_df.write \
  .format("org.apache.spark.sql.cassandra") \
  .options(table="lap_times", keyspace="f1") \
  .mode("overwrite") \
  .option("confirm.truncate", "true") \
  .save()

spark.stop()