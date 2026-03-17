"""
Irregular Subject Detection Pipeline (Secure Version)

Description:
Pipeline that queries an Oracle Data Warehouse to identify PDEs associated
with subjects that have a history of irregular inspections.

Sensitive data is protected using hashing and minimization techniques.

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

OUTPUT_PATH = "pde_subject_irregular_history.parquet"


# ---------------------------------------------------
# Utility functions
# ---------------------------------------------------

def get_current_time():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_connection():
    return oracledb.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dsn=DB_DSN
    )


# ---------------------------------------------------
# SQL Query (WITH DATA PROTECTION)
# ---------------------------------------------------

QUERY = """
WITH
PDE_HISTORY_IRREGULAR AS 
(
SELECT 
    mo.ID_PDE, 
    mo.ID_FORNECIMENTO,
    cs.ID_SUJEITO,
    STANDARD_HASH(cs.CPF_CNPJ, 'SHA256') AS CPF_CNPJ_HASH

FROM ORA_DW.MOV_OS mo

LEFT JOIN ORA_DW.CAD_FORNECIMENTO cf 
    ON cf.ID_FORNECIMENTO = mo.ID_FORNECIMENTO
    AND cf.DT_FIM = TO_DATE('9999-12-31','yyyy-mm-dd')

LEFT JOIN ORA_DW.CAD_SUJEITO cs
    ON cs.ID_SUJEITO = cf.ID_SUJEITO_TITULAR
    AND cs.DT_FIM = TO_DATE('9999-12-31','yyyy-mm-dd')

WHERE mo.ST_OS LIKE '%PROCEDENTE%'
AND cs.CPF_CNPJ IS NOT NULL
),

PDE_SUBJECT_HISTORY_IRREGULAR AS
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

AND STANDARD_HASH(cs.CPF_CNPJ, 'SHA256') IN
(
SELECT DISTINCT CPF_CNPJ_HASH
FROM PDE_HISTORY_IRREGULAR
)
),

PDE_FINAL AS
(
SELECT ID_PDE FROM PDE_HISTORY_IRREGULAR
UNION
SELECT ID_PDE FROM PDE_SUBJECT_HISTORY_IRREGULAR
)

SELECT DISTINCT ID_PDE, 'IRREGULAR' AS STATUS
FROM PDE_FINAL
"""


# ---------------------------------------------------
# Pipeline
# ---------------------------------------------------

def run_pipeline():

    print("Pipeline started:", get_current_time())

    start_time = time.time()

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(QUERY)

    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=columns)

    print("Rows before deduplication:", df.shape)

    df.drop_duplicates(inplace=True)

    print("Rows after deduplication:", df.shape)

    df.to_parquet(OUTPUT_PATH)

    cursor.close()
    connection.close()

    end_time = time.time()

    print("Output saved to:", OUTPUT_PATH)
    print("Execution time (minutes):", (end_time - start_time) / 60)
    print("Pipeline finished:", get_current_time())


# ---------------------------------------------------
# Run
# ---------------------------------------------------

if __name__ == "__main__":
    run_pipeline()