#Step 1 creating of airbnb_db on postgres local server AND 
#Data Loading
# A Python script (load_data.py) is used to load the dataset into the PostgreSQL table.

import pandas as pd
from sqlalchemy import create_engine

# Load dataset
df = pd.read_csv('path_to_your_file/AB_NYC_2019.csv')

# Database connection
engine = create_engine('postgresql://postgres:user@123@localhost:5432/airbnb_db')

# Load data into PostgreSQL
df.to_sql('airbnb_listings', engine, if_exists='replace', index=False)


#Step 2: ETL Process

#1.Data Extraction:
#Extract the data from the PostgreSQL database using SQLAlchemy

from sqlalchemy import create_engine
import pandas as pd

# Database connection
engine = create_engine('postgresql://postgres:user@123@localhost:5432/airbnb_db')

# Extract data
df = pd.read_sql('SELECT * FROM airbnb_listings', engine)

#2.Data Transformation:

#Normalize the data (e.g., separate date and time into different columns).
#Calculate additional metrics (e.g., average price per neighborhood).
#Handle missing values appropriately (e.g., fill, remove, or flag them).

# Normalize the data (e.g., separate date and time)
df['last_review'] = pd.to_datetime(df['last_review'])

# Calculate additional metrics (e.g., average price per neighborhood)
average_price_per_neighbourhood = df.groupby('neighbourhood')['price'].mean().reset_index()
average_price_per_neighbourhood.columns = ['neighbourhood', 'average_price']

# Handle missing values (e.g., fill, remove, or flag them)
df['reviews_per_month'] = df['reviews_per_month'].fillna(0)
df['last_review'] = df['last_review'].fillna(pd.Timestamp.now())

# Merge the average price per neighborhood back to the original dataframe
df = pd.merge(df, average_price_per_neighbourhood, on='neighbourhood', how='left')

#3.Data Loading:
#Load the transformed data into a new table in the PostgreSQL database.



# Create a new table for transformed data
engine.execute('''
CREATE TABLE IF NOT EXISTS transformed_airbnb_listings (
    id SERIAL PRIMARY KEY,
    name TEXT,
    host_id INTEGER,
    host_name TEXT,
    neighbourhood_group TEXT,
    neighbourhood TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    room_type TEXT,
    price INTEGER,
    minimum_nights INTEGER,
    number_of_reviews INTEGER,
    last_review TIMESTAMP,
    reviews_per_month DOUBLE PRECISION,
    calculated_host_listings_count INTEGER,
    availability_365 INTEGER,
    average_price DOUBLE PRECISION
);
''')

# Load transformed data into PostgreSQL
df.to_sql('transformed_airbnb_listings', engine, if_exists='replace', index=False)



#Step 3: Workflow Management with Metaflow
#Workflow Implementation:

#Use Metaflow to manage the ETL workflow.
#Implement steps in Metaflow to handle the ETL process from data ingestion to loading the transformed data into the PostgreSQL database.



from metaflow import FlowSpec, step, Parameter
import pandas as pd
from sqlalchemy import create_engine

class AirbnbETLFlow(FlowSpec):

    db_url = Parameter('db_url', default='postgresql://username:password@localhost:5432/airbnb_db')

    @step
    def start(self):
        self.next(self.extract_data)

    @step
    def extract_data(self):
        # Database connection
        engine = create_engine(self.db_url)

        # Extract data
        self.df = pd.read_sql('SELECT * FROM airbnb_listings', engine)
        self.next(self.transform_data)

    @step
    def transform_data(self):
        # Normalize the data (e.g., separate date and time)
        self.df['last_review'] = pd.to_datetime(self.df['last_review'])

        # Calculate additional metrics (e.g., average price per neighborhood)
        average_price_per_neighbourhood = self.df.groupby('neighbourhood')['price'].mean().reset_index()
        average_price_per_neighbourhood.columns = ['neighbourhood', 'average_price']

        # Handle missing values (e.g., fill, remove, or flag them)
        self.df['reviews_per_month'] = self.df['reviews_per_month'].fillna(0)
        self.df['last_review'] = self.df['last_review'].fillna(pd.Timestamp.now())

        # Merge the average price per neighborhood back to the original dataframe
        self.df = pd.merge(self.df, average_price_per_neighbourhood, on='neighbourhood', how='left')
        self.next(self.load_data)

    @step
    def load_data(self):
        # Database connection
        engine = create_engine(self.db_url)

        # Create a new table for transformed data if not exists
        engine.execute('''
        CREATE TABLE IF NOT EXISTS transformed_airbnb_listings (
            id SERIAL PRIMARY KEY,
            name TEXT,
            host_id INTEGER,
            host_name TEXT,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            room_type TEXT,
            price INTEGER,
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            last_review TIMESTAMP,
            reviews_per_month DOUBLE PRECISION,
            calculated_host_listings_count INTEGER,
            availability_365 INTEGER,
            average_price DOUBLE PRECISION
        );
        ''')

        # Load transformed data into PostgreSQL
        self.df.to_sql('transformed_airbnb_listings', engine, if_exists='replace', index=False)
        self.next(self.end)

    @step
    def end(self):
        print("ETL job completed successfully!")

if __name__ == '__main__':
    AirbnbETLFlow()



