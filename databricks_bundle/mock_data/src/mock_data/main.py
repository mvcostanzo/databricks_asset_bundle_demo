import faker
import polars as pl

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
    GenerateFakeDataset.generate_data(25)