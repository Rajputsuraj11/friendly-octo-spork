"""
Healthcare Claims Processing ETL Pipeline
Extracts data from hospital database, transforms it, and loads to Azure SQL
"""

import pandas as pd
import pyodbc
from typing import List, Dict, Any


class HealthcareETL:
    """ETL pipeline for healthcare claims processing"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        
    def extract_claims_data(self, query: str) -> pd.DataFrame:
        """
        Extract claims data from hospital database
        
        Args:
            query: SQL query to extract claims data
            
        Returns:
            DataFrame containing claims data
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            raise Exception(f"Error extracting data: {str(e)}")
    
    def transform_claims_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform claims data by cleaning and standardizing
        
        Args:
            df: Raw claims DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Standardize date format
        if 'claim_date' in df.columns:
            df['claim_date'] = pd.to_datetime(df['claim_date'])
            
        # Clean amount fields
        if 'claim_amount' in df.columns:
            df['claim_amount'] = pd.to_numeric(df['claim_amount'], errors='coerce')
            
        return df
    
    def load_to_azure_sql(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Load transformed data to Azure SQL
        
        Args:
            df: Transformed DataFrame
            table_name: Target table name in Azure SQL
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Create table if not exists (simplified example)
            create_table_query = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            CREATE TABLE {table_name} (
                claim_id INT PRIMARY KEY,
                patient_id INT,
                claim_date DATE,
                claim_amount DECIMAL(10,2),
                claim_status VARCHAR(50)
            )
            """
            cursor.execute(create_table_query)
            
            # Insert data
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (claim_id, patient_id, claim_date, claim_amount, claim_status)
                VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(insert_query, 
                             row.get('claim_id'),
                             row.get('patient_id'),
                             row.get('claim_date'),
                             row.get('claim_amount'),
                             row.get('claim_status', 'PENDING'))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")
    
    def run_etl_pipeline(self, extract_query: str, target_table: str) -> bool:
        """
        Run complete ETL pipeline
        
        Args:
            extract_query: SQL query for data extraction
            target_table: Target table name for loading
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract
            raw_data = self.extract_claims_data(extract_query)
            
            # Transform
            transformed_data = self.transform_claims_data(raw_data)
            
            # Load
            success = self.load_to_azure_sql(transformed_data, target_table)
            
            return success
            
        except Exception as e:
            raise Exception(f"ETL pipeline failed: {str(e)}")


if __name__ == "__main__":
    # Example usage
    connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_user;PWD=your_password"
    
    etl = HealthcareETL(connection_string)
    
    # Example query
    query = """
    SELECT claim_id, patient_id, claim_date, claim_amount, claim_status
    FROM hospital_claims
    WHERE claim_date >= DATEADD(day, -30, GETDATE())
    """
    
    # Run pipeline
    try:
        success = etl.run_etl_pipeline(query, "processed_claims")
        if success:
            print("ETL pipeline completed successfully!")
        else:
            print("ETL pipeline failed!")
    except Exception as e:
        print(f"Error: {str(e)}")
