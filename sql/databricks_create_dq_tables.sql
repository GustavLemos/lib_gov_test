-- =========================================================
-- gov_credit - Data Quality Governance Tables (Databricks)
-- =========================================================
-- Este script cria as tabelas de governança de dados
-- utilizadas pela biblioteca gov_credit.
-----------------------------------------

-- IMPORTANTE:
-- - Execute uma única vez por workspace
-- - Ajuste o catalog/schema se usar Unity Catalog
-- =========================================================

CREATE SCHEMA IF NOT EXISTS dq;

-- =========================================================
-- DATASETS GOVERNADOS
-- =========================================================
CREATE TABLE IF NOT EXISTS dq.datasets (
dataset_id    STRING,
dataset_name  STRING,
dataset_path  STRING,
format        STRING,
owner         STRING,
domain        STRING,
created_at    TIMESTAMP,
updated_at    TIMESTAMP,
active        BOOLEAN
)
USING DELTA;

-- =========================================================
-- COLUNAS DOS DATASETS
-- =========================================================
CREATE TABLE IF NOT EXISTS dq.columns (
dataset_id   STRING,
column_name  STRING,
data_type    STRING,
nullable     BOOLEAN,
created_at   TIMESTAMP,
updated_at   TIMESTAMP,
active       BOOLEAN
)
USING DELTA;

-- =========================================================
-- REGRAS DE QUALIDADE
-- =========================================================
CREATE TABLE IF NOT EXISTS dq.rules (
rule_id         STRING,
dataset_id      STRING,
column_name     STRING,   -- NULL para regras de dataset
rule_type       STRING,   -- not_null, range, regex, custom
rule_definition STRING,   -- JSON serializado
severity        STRING,   -- warn | error
active          BOOLEAN,
created_at      TIMESTAMP,
updated_at      TIMESTAMP
)
USING DELTA;

-- =========================================================
-- RESULTADOS DAS VALIDAÇÕES
-- =========================================================
CREATE TABLE IF NOT EXISTS dq.results (
execution_id   STRING,
dataset_id     STRING,
column_name    STRING,
rule_id        STRING,
rule_type      STRING,
status         STRING,   -- pass | fail
failed_rows    BIGINT,
total_rows     BIGINT,
execution_time TIMESTAMP
)
USING DELTA;
