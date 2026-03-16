# Irregular Subject Detection Pipeline

## Overview

This project implements a **production-style data pipeline** designed to identify **PDE records associated with subjects that have a history of irregular inspections**.

The pipeline connects to an **Oracle Data Warehouse**, executes a structured SQL query, processes the results using **Python and Pandas**, and exports the final dataset in **Parquet format** for downstream analytics and data processing.

This repository demonstrates key **Data Engineering practices**, including:

* Secure database connectivity
* SQL-based data extraction
* ETL pipeline development
* Data deduplication and transformation
* Efficient columnar data storage
* Environment-based credential management

---

# Architecture

The pipeline follows a simplified **ETL workflow**.

```
Oracle Data Warehouse
        │
        │ (SQL Query)
        ▼
Python Data Pipeline
        │
        │ (Pandas Processing)
        ▼
Parquet Dataset
        │
        ▼
Analytics / Data Science / BI
```

---

# Project Structure

```
irregular-subject-detection-pipeline
│
├── irregular_subject_pipeline.py   # Main ETL pipeline
├── requirements.txt                # Project dependencies
├── README.md                       # Documentation
├── .env.example                    # Environment variable template
└── .gitignore                      # Ignored files
```

---

# Tech Stack

| Technology | Purpose                 |
| ---------- | ----------------------- |
| Python     | Pipeline development    |
| Pandas     | Data processing         |
| OracleDB   | Data source             |
| oracledb   | Database connectivity   |
| PyArrow    | Parquet file generation |

---

# Key Features

## Secure Credential Management

Database credentials are **not stored in the codebase**.
All sensitive information is loaded using **environment variables**.

```
DB_USER
DB_PASSWORD
DB_DSN
```

This prevents credential leakage and aligns with **security best practices**.

---

## Efficient Data Storage

The pipeline exports results in **Parquet format**, which provides:

* Columnar storage
* Efficient compression
* Faster analytical queries
* Compatibility with Spark, Pandas, and modern data platforms

---

## Data Processing

The pipeline performs:

1. SQL-based extraction from Oracle DW
2. Loading results into a Pandas DataFrame
3. Deduplication of records
4. Export to Parquet format

---

# Installation

Clone the repository:

```
git clone https://github.com/your-username/irregular-subject-detection-pipeline.git
```

Navigate to the project directory:

```
cd irregular-subject-detection-pipeline
```

Install dependencies:

```
pip install -r requirements.txt
```

---

# Environment Configuration

Create a `.env` file in the project root.

Example configuration:

```
DB_USER=your_database_user
DB_PASSWORD=your_secure_password
DB_DSN=host:port/service
```

The project uses **python-dotenv** to load these variables.

---

# Running the Pipeline

Execute the pipeline using:

```
python irregular_subject_pipeline.py
```

During execution, the pipeline will:

* Connect to Oracle
* Execute the irregular subject detection query
* Process the data
* Remove duplicate records
* Export the dataset to Parquet

---

# Output

Generated dataset:

```
pde_sujeito_hist_irreg.parquet
```

Schema:

| Column    | Description                               |
| --------- | ----------------------------------------- |
| ID_PDE    | Unique identifier of the PDE              |
| IRREGULAR | Flag indicating irregular subject history |

---

# Performance Considerations

The pipeline is optimized for:

* Direct SQL filtering to minimize data transfer
* Columnar output format (Parquet)
* Deduplication at the DataFrame level

For larger datasets, the following improvements may be implemented:

* Chunked data extraction
* Parallel processing
* Distributed execution with Spark

---

# Security Best Practices

* Credentials stored in environment variables
* `.env` files excluded from version control
* No sensitive information stored in the repository

---

# Future Improvements

Possible enhancements include:

* Logging and monitoring
* Structured configuration management
* Pipeline orchestration (Airflow / Prefect)
* Containerization with Docker
* CI/CD integration
* Automated data quality checks
* Unit and integration testing

---

# Author

Developed as a **Data Engineering portfolio project** demonstrating ETL pipeline design, database integration, and data processing workflows.

---
