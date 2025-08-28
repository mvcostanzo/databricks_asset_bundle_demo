from pyspark.sql import SparkSession

def main():
    sparkSession = SparkSession.builder.appName('ClearTables').getOrCreate()
    sparkSession.sql("DROP TABLE IF EXISTS health_data.person.people")