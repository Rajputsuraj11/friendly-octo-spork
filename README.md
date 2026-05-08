# Healthcare Claims Processing ETL Pipeline

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline for processing healthcare claims data from hospital databases to Azure SQL.

## Architecture
```
Hospital DB → Python ETL → Azure SQL
```

## Features
- **Extract**: Pull claims data from hospital database using SQL queries
- **Transform**: Clean and standardize claims data (date formatting, amount validation, duplicate removal)
- **Load**: Load processed data to Azure SQL database
- **CI/CD**: Automated testing and linting with GitHub Actions

## CI/CD Pipeline
The project includes a GitHub Actions workflow that:
- Triggers on pull requests
- Runs flake8 linting for code quality
- Executes pytest for unit testing
- Generates coverage reports
- Uploads coverage to Codecov

## Requirements
- Python 3.9+
- pandas
- pyodbc
- pytest
- flake8
- coverage

## Installation
```bash
pip install -r requirements.txt
```

## Running Tests
```bash
pytest --cov=.
```

## Running Linting
```bash
flake8 .
```

## Usage
```python
from etl_pipeline import HealthcareETL

# Initialize ETL pipeline
etl = HealthcareETL(connection_string)

# Define extraction query
query = """
SELECT claim_id, patient_id, claim_date, claim_amount, claim_status
FROM hospital_claims
WHERE claim_date >= DATEADD(day, -30, GETDATE())
"""

# Run complete pipeline
success = etl.run_etl_pipeline(query, "processed_claims")
```

## GitHub Actions Workflow
The CI pipeline is automatically triggered on pull requests and includes:
1. Code checkout
2. Python environment setup
3. Dependency installation
4. Code linting with flake8
5. Unit testing with pytest
6. Coverage reporting

## Learning Outcomes
- AI-generated YAML workflows
- GitHub Actions automation
- CI/CD best practices for Python projects
- Healthcare data processing patterns
