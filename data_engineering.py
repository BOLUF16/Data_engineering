from dataclasses import dataclass
import pandas as pd
import numpy as np
import datetime
from faker import Faker
from faker_datasets import Provider, add_dataset, with_datasets
import random
import os



fake = Faker()

@add_dataset("cities", "cities.json")
class Cities(Provider):

    @with_datasets("cities")
    def city(self, cities):
        return self.__pick__(cities)
    
fake.add_provider(Cities) 

#configuration
@dataclass
class Datapipelineconfig:
    
    """configuration class for data paths"""
    
    base_path: str = "data"
    dimcustomer_save_path: str = os.path.join(base_path, "dim_customer.csv")
    dimdate_save_path : str = os.path.join(base_path, "dim_date.csv")
    dimlocation_save_path : str = os.path.join(base_path, "dim_location.csv")
    factinteraction_save_path : str = os.path.join(base_path, "fact_interactions.csv")

class DataGenerator:
    """class to handle all data generation logic"""
    def __init__(self, config: Datapipelineconfig):
        self.start_date = datetime.datetime(2023,1,1)
        self.end_date = datetime.datetime(2024, 12, 31)
        self.config = config

    
    @staticmethod
    def fake_industry_type() -> str:
        industry_type = ["Technology", "Healthcare", "Finance", "Retail", 
                        "Manufacturing", "Education", "Entertainment"]
        return random.choice(industry_type)
    
    @staticmethod
    def fake_subscription_plan(company_size):
        if company_size == "Small size":
            return np.random.choice(
                ["Free", "Individual pro", "Business pro"],
                p=[0.5, 0.3, 0.2]  
            )
        elif company_size == "Medium size":
            return np.random.choice(
                ["Free", "Individual pro", "Business pro"],
                p=[0.2, 0.3, 0.5]  # Probabilities for medium size
            )

    
    def generate_customer_data(self, num_rows: int = 1000) -> pd.DataFrame:
        """generate dimension table called dim_customer"""
        
        data = {
            "customer_id" : range(1,num_rows + 1),
            "company_name" : [fake.company() for _ in range(num_rows)],
            "industry": [self.fake_industry_type() for _ in range(num_rows)],
            "company_size" : ["Medium size" if random.randrange(1,100) > 20
                              else "Small size" for _ in range(num_rows)],
            "registration_date" : [fake.date_between(start_date=self.start_date,
                                                          end_date=self.end_date) for _ in range(num_rows)],
            "email" : [fake.company_email() for _ in range(num_rows)],
            "phone_number" : [fake.phone_number() for _ in range(num_rows)]
        } 

        df = pd.DataFrame(data)
        df["subscription_plan"] = df["company_size"].apply(self.fake_subscription_plan)

        return df
    
    def generate_date_dimension(self, start_date: datetime.date, 
                              end_date: datetime.date) -> pd.DataFrame:
        """Generate dimension table called dim_date"""
        date_list = [start_date + datetime.timedelta(days=x) 
                    for x in range((end_date - start_date).days + 1)]
        data = {
            "date_id": range(1, len(date_list) + 1),
            "date": date_list,
            "day": [i.day for i in date_list],
            "month": [i.month for i in date_list],
            "year": [i.year for i in date_list],
            "weekday": [i.weekday() for i in date_list]
        }
        df = pd.DataFrame(data)
        weekday_map = {
            0: "Monday", 1: "Tuesday", 2: "Wednesday",
            3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"
        }
        df["weekday"] = df["weekday"].replace(weekday_map)
        return df
    
    def generate_location_data(self, num_records: int = 300) -> pd.DataFrame:
        """Generate dimension table called dim_location"""
        locations = {}
        for i in range(1, num_records + 1):
            city = fake.city()
            locations[i] = {
                "location_id": i,
                "address":fake.address(),
                "city": "{name}".format(**city),
                "state": "{state_name}".format(**city),
                "country": "{country_name}".format(**city)
            }
        return pd.DataFrame(locations).T
    
    def generate_fact_interaction(self, num_rows: int = 10000):
        """Generate fact interactions data ensuring interaction dates are after registration dates"""
    
        dim_customer = pd.read_csv(self.config.dimcustomer_save_path)
        dim_date = pd.read_csv(self.config.dimdate_save_path)
        
        # Convert date column to datetime once for better performance
        dim_date['date'] = pd.to_datetime(dim_date['date'])
        dim_customer['registration_date'] = pd.to_datetime(dim_customer['registration_date'])
        
        interaction_types = [
            "View Report", "Dashboard Interaction", "Update Financial Data", 
            "Download Report", "Customer Support Request",  
            "Onboarding Session", "Schedule Financial Task", "View Transaction History", "Submit Tax Filing"
        ]
        
        associated_products = {
            "View Report": "Analytics",
            "Dashboard Interaction": "Analytics",
            "Update Financial Data": "Bookkeeping",
            "Download Report": "Analytics",
            "Customer Support Request": "Support Services",
            "Onboarding Session": "Onboarding Services",
            "Schedule Financial Task": "Bookkeeping",
            "View Transaction History": "Transactions",
            "Submit Tax Filing": "Tax Services"
        }
        
        features_used = {
            "Analytics": ["Reports", "Charts", "Graphs", "Dashboard", "Visualizations"],
            "Bookkeeping": ["Expense Upload", "Invoice Creation", "Ledger Review", "Task Scheduling"],
            "Support Services": ["Chat Support", "Email Support", "Feedback Form"],
            "Onboarding Services": ["Setup Wizard", "Training Materials"],
            "Transactions": ["Transaction Log", "Filters", "Export Data"],
            "Tax Services": ["Tax Filing Module", "Compliance Checks"]
        }
        
        interactions = []
        
        for _ in range(num_rows):
            customer_id = random.randrange(1, 1001)
            
            # Get customer registration date
            customer_reg_date = dim_customer.loc[
                dim_customer['customer_id'] == customer_id, 'registration_date'
            ].iloc[0]
            
           
            valid_dates = dim_date[dim_date['date'] > customer_reg_date]
            
            if len(valid_dates) == 0:
                continue  
                
           
            interaction_date_id = random.choice(valid_dates['date_id'].values)
            
            interaction = {
                "interaction_type": random.choice(interaction_types),
                "customer_id": customer_id,
                "interaction_date_id": interaction_date_id,
                "duration_minutes": random.randint(1, 60),
                "location_id": random.randrange(1, 101),
                "channels": random.choice(["Web App", "Mobile App"])
            }
            
            interactions.append(interaction)
        
        
        df = pd.DataFrame(interactions)
        df['interaction_id'] = range(1, len(df) + 1)
        df["associated_product"] = df["interaction_type"].map(associated_products)
        df["features_used"] = df["associated_product"].apply(lambda x: random.choice(features_used[x]))
        df["outcome"] = df["interaction_type"].apply(
            lambda x: random.choice(["Resolved", "pending"]) if x == "Customer Support Request" else "Completed"
        )
        df["action_taken"] = df.apply(
            lambda x: f'Performed {x["interaction_type"].lower()} using {x["features_used"]}', 
            axis=1
        )
        
        # Reorder columns
        df = df[[
            "interaction_id", "customer_id", "location_id", "interaction_type",
            "interaction_date_id", "channels", "duration_minutes", "action_taken",
            "associated_product", "features_used", "outcome"
        ]]
        
        return df
            
            
class Datapipeline:
    """class to data pipeline creation"""

    def __init__(self, config: Datapipelineconfig):
        self.config = config
        self.generator = DataGenerator(config)
        
    def create_directory_structure(self):
        """Create necessary directories"""
        os.makedirs(self.config.base_path, exist_ok=True)
        
    def generate_and_save_dimensions_and_facts(self):
        """Generate and save all dimension tables"""
        # Generate and save customer dimension
        dim_customer = self.generator.generate_customer_data()
        dim_customer.to_csv(self.config.dimcustomer_save_path, index=False)
        
        # Generate and save date dimension
        dim_date = self.generator.generate_date_dimension(
            datetime.date(2023, 1, 1), 
            datetime.date(2024, 12, 31)
        )
        dim_date.to_csv(self.config.dimdate_save_path, index=False)
        
        # Generate and save location dimension
        dim_location = self.generator.generate_location_data()
        dim_location.to_csv(self.config.dimlocation_save_path, index=False)
        
        
        # Generate and save fact interactions
        fact_interactions = self.generator.generate_fact_interaction()
        fact_interactions.to_csv(self.config.factinteraction_save_path, index=False)
        

def main():
    """Main execution function"""
    # Initialize configuration
    config = Datapipelineconfig()
    
    # Initialize data warehouse
    store = Datapipeline(config)
    
    # Create directory structure
    store.create_directory_structure()
    
    # Generate and save dimension tables
    store.generate_and_save_dimensions_and_facts()

if __name__ == "__main__":
    main()