"""
Test suite for Healthcare Claims Processing ETL Pipeline
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from etl_pipeline import HealthcareETL


class TestHealthcareETL:
    """Test cases for HealthcareETL class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.connection_string = "test_connection_string"
        self.etl = HealthcareETL(self.connection_string)
        
        # Sample test data
        self.sample_data = pd.DataFrame({
            'claim_id': [1, 2, 3],
            'patient_id': [101, 102, 103],
            'claim_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'claim_amount': ['100.50', '200.75', '150.25'],
            'claim_status': ['PENDING', 'APPROVED', 'REJECTED']
        })
    
    def test_init(self):
        """Test ETL initialization"""
        assert self.etl.connection_string == self.connection_string
    
    @patch('etl_pipeline.pyodbc.connect')
    def test_extract_claims_data_success(self, mock_connect):
        """Test successful data extraction"""
        # Mock database connection and query execution
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock pandas read_sql
        with patch('etl_pipeline.pd.read_sql', return_value=self.sample_data):
            result = self.etl.extract_claims_data("SELECT * FROM claims")
            
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_connect.assert_called_once_with(self.connection_string)
    
    @patch('etl_pipeline.pyodbc.connect')
    def test_extract_claims_data_failure(self, mock_connect):
        """Test data extraction failure"""
        mock_connect.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception) as exc_info:
            self.etl.extract_claims_data("SELECT * FROM claims")
        
        assert "Error extracting data" in str(exc_info.value)
    
    def test_transform_claims_data(self):
        """Test data transformation"""
        # Test with sample data that needs cleaning
        dirty_data = pd.DataFrame({
            'claim_id': [1, 1, 2],  # Duplicate
            'patient_id': [101, 101, 102],
            'claim_date': ['2023-01-01', '2023-01-01', 'invalid_date'],
            'claim_amount': ['100.50', '100.50', 'invalid_amount'],
            'claim_status': ['PENDING', 'PENDING', 'APPROVED']
        })
        
        result = self.etl.transform_claims_data(dirty_data)
        
        # Check duplicates removed
        assert len(result) == 2
        
        # Check date conversion (valid dates should be converted, invalid should remain)
        assert pd.api.types.is_datetime64_any_dtype(result['claim_date'])
        
        # Check amount conversion (valid amounts should be numeric, invalid should be NaN)
        assert pd.api.types.is_numeric_dtype(result['claim_amount'])
    
    def test_transform_claims_data_empty(self):
        """Test transformation with empty DataFrame"""
        empty_df = pd.DataFrame()
        result = self.etl.transform_claims_data(empty_df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    @patch('etl_pipeline.pyodbc.connect')
    def test_load_to_azure_sql_success(self, mock_connect):
        """Test successful data loading to Azure SQL"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.etl.load_to_azure_sql(self.sample_data, "test_table")
        
        assert result is True
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('etl_pipeline.pyodbc.connect')
    def test_load_to_azure_sql_failure(self, mock_connect):
        """Test data loading failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            self.etl.load_to_azure_sql(self.sample_data, "test_table")
        
        assert "Error loading data" in str(exc_info.value)
    
    @patch.object(HealthcareETL, 'load_to_azure_sql')
    @patch.object(HealthcareETL, 'transform_claims_data')
    @patch.object(HealthcareETL, 'extract_claims_data')
    def test_run_etl_pipeline_success(self, mock_extract, mock_transform, mock_load):
        """Test complete ETL pipeline success"""
        mock_extract.return_value = self.sample_data
        mock_transform.return_value = self.sample_data
        mock_load.return_value = True
        
        result = self.etl.run_etl_pipeline("SELECT * FROM claims", "target_table")
        
        assert result is True
        mock_extract.assert_called_once_with("SELECT * FROM claims")
        mock_transform.assert_called_once_with(self.sample_data)
        mock_load.assert_called_once_with(self.sample_data, "target_table")
    
    @patch.object(HealthcareETL, 'extract_claims_data')
    def test_run_etl_pipeline_extraction_failure(self, mock_extract):
        """Test ETL pipeline failure during extraction"""
        mock_extract.side_effect = Exception("Extraction failed")
        
        with pytest.raises(Exception) as exc_info:
            self.etl.run_etl_pipeline("SELECT * FROM claims", "target_table")
        
        assert "ETL pipeline failed" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__])
