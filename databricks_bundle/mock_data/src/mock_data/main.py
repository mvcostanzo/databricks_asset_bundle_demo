import faker
import polars as pl
from pyspark.sql import SparkSession

class GenerateFakeDataset:
    @staticmethod
    def generate_data(number_items: int) -> pl.DataFrame:
        fake = faker.Faker()
    
        dataset = {
            'id': [i for i in range(1, number_items+1)],
            'full_name': [fake.name() for _ in range(number_items)],
            'address' : [fake.address() for _ in range(number_items)],
            'birthdate': [fake.date_of_birth(minimum_age=18, maximum_age=95) for _ in range(number_items)],
            'phone': [fake.basic_phone_number() for _ in range(number_items)]
        }
        return pl.DataFrame(dataset)
    
def main():
    sparkSession = SparkSession.builder.appName('LoadMockData').getOrCreate()
    fake_dataset = GenerateFakeDataset.generate_data(10000)
    spark_df = sparkSession.createDataFrame(fake_dataset.to_pandas())
    spark_df.write.format("delta").mode('append').saveAsTable('health_data.people.patients')