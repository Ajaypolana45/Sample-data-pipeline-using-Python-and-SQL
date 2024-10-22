# Sample-data-pipeline-using-Python-and-SQL
This script highlights skills like data processing with pandas, using SQL for database operations, and integrating Python and SQL.
import pandas as pd
import sqlite3

# Step 1: Extract - Read data from a CSV file
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print("Data extraction successful.")
        return df
    except Exception as e:
        print(f"Error during data extraction: {e}")
        return None

# Step 2: Transform - Clean and process the data
def transform_data(df):
    try:
        # Example transformation: Removing duplicates and handling missing values
        df = df.drop_duplicates()
        df = df.fillna(method='ffill')  # Forward fill missing values

        # Adding a new calculated column
        df['Total'] = df['Quantity'] * df['Price']

        print("Data transformation successful.")
        return df
    except Exception as e:
        print(f"Error during data transformation: {e}")
        return None

# Step 3: Load - Load the processed data into a SQLite database
def load_data(df, database_name, table_name):
    try:
        # Establish a connection to the database
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                ProductID INTEGER,
                ProductName TEXT,
                Quantity INTEGER,
                Price REAL,
                Total REAL
            )
        ''')

        # Insert data into the table
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()

        print("Data loading successful.")
    except Exception as e:
        print(f"Error during data loading: {e}")
    finally:
        conn.close()

# Main function to run the ETL pipeline
if __name__ == "__main__":
    # Define file paths and database/table names
    csv_file_path = 'data/sample_sales_data.csv'
    database_name = 'sales_data.db'
    table_name = 'Sales'

    # Execute ETL process
    data = extract_data(csv_file_path)
    if data is not None:
        transformed_data = transform_data(data)
        if transformed_data is not None:
            load_data(transformed_data, database_name, table_name)
