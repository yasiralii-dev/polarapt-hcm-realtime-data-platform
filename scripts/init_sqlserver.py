import os
import pytds

host = os.environ["DB_HOST"]
pw = os.environ["DB_PASS"]

conn = pytds.connect(host, user="hcmadmin", password=pw, database="master",
                     autocommit=True, login_timeout=30)
conn.cursor().execute("IF DB_ID('hcm_payroll') IS NULL CREATE DATABASE hcm_payroll")
conn.close()

conn = pytds.connect(host, user="hcmadmin", password=pw, database="hcm_payroll",
                     autocommit=True, login_timeout=30)
conn.cursor().execute("""
IF OBJECT_ID('dbo.fx_rates', 'U') IS NULL
CREATE TABLE dbo.fx_rates (
    id             INT IDENTITY(1,1) PRIMARY KEY,
    base_currency  CHAR(3)        NOT NULL,
    currency       CHAR(3)        NOT NULL,
    rate           DECIMAL(18,6)  NOT NULL,
    rate_timestamp DATETIME2      NOT NULL,
    source_key     NVARCHAR(200)  NOT NULL,
    ingested_at    DATETIME2      NOT NULL,
    CONSTRAINT uq_fx UNIQUE (source_key, currency)
)
""")
print("OK: database hcm_payroll and table dbo.fx_rates ready")
