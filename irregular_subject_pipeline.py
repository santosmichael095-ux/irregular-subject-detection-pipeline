"""
Irregular Subject Detection Pipeline

Description:
Pipeline that queries an Oracle Data Warehouse to identify PDEs associated
with subjects that have a history of irregular inspections.

Sensitive data such as credentials are loaded from environment variables.

Required environment variables:
DB_USER
DB_PASSWORD
DB_DSN
"""

import os
import time
import datetime as dt

import pandas as pd
import oracledb
from dotenv import load_dotenv


# ---------------------------------------------------
# Load environment variables
# ---------------------------------------------------

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DSN = os.getenv("DB_DSN")

OUTPUT_PATH = "pde_sujeito_hist_irreg.parquet"


# ---------------------------------------------------
# Utility functions
# ---------------------------------------------------

def current_time():
    """Return formatted current timestamp."""
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_connection():
    """Create Oracle database connection."""
    return oracledb.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dsn=DB_DSN
    )


# ---------------------------------------------------
# SQL Query
# ---------------------------------------------------

QUERY = """
WITH
PDE_HIST_IRREG AS 
(
SELECT mo.ID_PDE, mo.ID_FORNECIMENTO, cs.ID_SUJEITO, cs.CPF_CNPJ
FROM ORA_DW.MOV_OS mo
LEFT JOIN ORA_DW.CAD_FORNECIMENTO cf 
    ON cf.ID_FORNECIMENTO = mo.ID_FORNECIMENTO
    AND cf.DT_FIM = TO_DATE('9999-12-31','yyyy-mm-dd')

LEFT JOIN ORA_DW.CAD_SUJEITO cs
    ON cs.ID_SUJEITO = cf.ID_SUJEITO_TITULAR
    AND cs.DT_FIM = TO_DATE('9999-12-31','yyyy-mm-dd')

WHERE mo.ST_OS LIKE '%PROCEDENTE%'
),

PDE_SUJEITO_HIST_IRREG AS
(
SELECT cp.ID_PDE

FROM ORA_DW.CAD_PDE cp

LEFT JOIN ORA_DW.CAD_FORNECIMENTO cf
    ON cf.ID_FORNECIMENTO = cp.ID_FORNECIMENTO
    AND cf.DT_FIM = TO_DATE('9999-12-31','yyyy-mm-dd')

LEFT JOIN ORA_DW.CAD_SUJEITO cs
    ON cs.ID_SUJEITO = cf.ID_SUJEITO_TITULAR
    AND cs.DT_FIM = TO_DATE('9999-12-31','yyyy-mm-dd')

WHERE cp.ID_FORNECIMENTO IS NOT NULL

AND cs.CPF_CNPJ IN
(
SELECT DISTINCT CPF_CNPJ
FROM PDE_HIST_IRREG
WHERE CPF_CNPJ <> '-1'
)
),

PDE_SUJEITO_HIST_IRREG_FINAL AS
(
SELECT ID_PDE FROM PDE_HIST_IRREG
UNION
SELECT ID_PDE FROM PDE_SUJEITO_HIST_IRREG
)

SELECT DISTINCT ID_PDE, 'IRREGULAR' AS IRREGULAR
FROM PDE_SUJEITO_HIST_IRREG_FINAL
"""


# ---------------------------------------------------
# Pipeline
# ---------------------------------------------------

def run_pipeline():

    print("Pipeline started:", current_time())

    start_time = time.time()

    # Connect to database
    connection = get_connection()
    cursor = connection.cursor()

    # Execute query
    cursor.execute(QUERY)

    # Fetch column names
    columns = [col[0] for col in cursor.description]

    # Fetch data
    data = cursor.fetchall()

    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)

    print("Rows before deduplication:", df.shape)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    print("Rows after deduplication:", df.shape)

    # Save output
    df.to_parquet(OUTPUT_PATH)

    # Close connection
    cursor.close()
    connection.close()

    end_time = time.time()

    print("Output saved to:", OUTPUT_PATH)
    print("Execution time (minutes):", (end_time - start_time) / 60)
    print("Pipeline finished:", current_time())


# ---------------------------------------------------
# Run
# ---------------------------------------------------

if __name__ == "__main__":
    run_pipeline()
