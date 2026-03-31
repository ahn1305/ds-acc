class SnowflakeSQLGenerator:

    def __init__(self, parsed):
        self.stages = parsed["stages"]
        self.links = parsed["links"]

    # =========================================
    # BUILD STAGING MODELS
    # =========================================
    def build_staging(self):

        staging = {}

        for stage_name, stage in self.stages.items():

            if stage["type"] in ["PxDataSet", "PxSequentialFile", "PxOracleConnector"]:

                cols = [c["name"] for c in stage["outputs"]]

                select_cols = ",\n    ".join(cols)

                sql = f"""
{{{{ config(materialized='view') }}}}

SELECT
    {select_cols}
FROM RAW_{stage_name.upper()}
"""

                staging[stage_name] = sql.strip()

        return staging

    # =========================================
    # BUILD JOINS
    # =========================================
    def build_joins(self):

        joins = {}

        for stage_name, stage in self.stages.items():

            if stage["type"] == "PxJoin":

                inputs = stage["inputs"]

                if len(inputs) < 2:
                    continue

                left = inputs[0]
                right = inputs[1]

                sql = f"""
{{{{ config(materialized='table') }}}}

WITH left_table AS (
    SELECT * FROM {{{{ ref('{left}') }}}}
),
right_table AS (
    SELECT * FROM {{{{ ref('{right}') }}}}
)

SELECT *
FROM left_table l
JOIN right_table r
ON l.branch_code = r.dim_branch_code
"""

                joins[stage_name] = sql.strip()

        return joins

    # =========================================
    # LOOKUP → LEFT JOIN
    # =========================================
    def build_lookup(self):

        lookups = {}

        for stage_name, stage in self.stages.items():

            if stage["type"] == "PxLookup":

                inputs = stage["inputs"]

                if len(inputs) < 2:
                    continue

                driver = inputs[0]
                ref = inputs[1]

                sql = f"""
{{{{ config(materialized='table') }}}}

WITH driver AS (
    SELECT * FROM {{{{ ref('{driver}') }}}}
),
ref_table AS (
    SELECT * FROM {{{{ ref('{ref}') }}}}
)

SELECT d.*, r.*
FROM driver d
LEFT JOIN ref_table r
ON d.customer_segment = r.ref_customer_segment
AND d.risk_category = r.ref_risk_category
"""

                lookups[stage_name] = sql.strip()

        return lookups

    # =========================================
    # TRANSFORMER → FINAL LOGIC
    # =========================================
    def build_transformer(self):

        transformers = {}

        for stage_name, stage in self.stages.items():

            if stage["type"] == "PxTransformer":

                inputs = stage["inputs"]

                if not inputs:
                    continue

                source = inputs[0]

                select_lines = []

                for col in stage["outputs"]:
                    name = col["name"]
                    derivation = col.get("derivation")

                    if derivation:
                        sql_expr = self.convert_to_sql(derivation)
                        select_lines.append(f"{sql_expr} AS {name}")
                    else:
                        select_lines.append(name)

                sql = f"""
{{{{ config(materialized='table') }}}}

SELECT
    {",\n    ".join(select_lines)}
FROM {{{{ ref('{source}') }}}}
"""

                transformers[stage_name] = sql.strip()

        return transformers

    # =========================================
    # DERIVATION → SQL
    # =========================================
    def convert_to_sql(self, derivation):

        if not derivation:
            return "NULL"

        # Basic conversions
        sql = derivation

        sql = sql.replace("If", "CASE WHEN")
        sql = sql.replace("Then", "THEN")
        sql = sql.replace("Else", "ELSE")

        # handle end
        if "CASE WHEN" in sql:
            sql += " END"

        # concat
        sql = sql.replace(":", "||")

        return sql

    # =========================================
    # FINAL MART
    # =========================================
    def build_mart(self):

        return {
            "fact_banking_transactions": """
{{ config(materialized='incremental', unique_key='fact_id') }}

SELECT *
FROM {{ ref('Transformer_BusinessRules') }}
"""
        }

    # =========================================
    # RUN ALL
    # =========================================
    def run(self):

        return {
            "staging": self.build_staging(),
            "intermediate": {
                **self.build_joins(),
                **self.build_lookup()
            },
            "marts": {
                **self.build_transformer(),
                **self.build_mart()
            }
        }
