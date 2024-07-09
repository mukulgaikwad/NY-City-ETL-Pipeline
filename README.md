# NY-City-ETL-Pipeline
The goal of this project is to design and implement a scalable ETL pipeline and work with databases.

1.Data Ingestion and Storage - Set up a PostgreSQL database on local server and create a airbnb_db.<Br>
2.Python script to load the dataset into the PostgreSQL table<br>
3.Extract the data from the PostgreSQL database using SQLAlchemy using python code.<br>
4.Data Transformation - Perform the necessary transformations<br>
(Normalize data,Calculate additional metrics (e.g., average price per neighborhood),handle missing values,<br>
Merge the average price per neighborhood back to the original dataframe)<br>
5.Data Loading - Load the transformed data into a new table in the PostgreSQL database<br>
6.Workflow Management with Metaflow - Workflow Implementation<br>

Tips:<br>
1.Ensure PostgreSQL is Running: Make sure your PostgreSQL server is up and running and that you can connect to it using your credentials.<br>
2.Check Database and Table Names: Ensure that the database airbnb_db exists and the table names match what you created in PostgreSQL. <br>

***end***
