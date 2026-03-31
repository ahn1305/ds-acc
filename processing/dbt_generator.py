import os

def build_dbt_project(sttm, parsed):

    model_name = parsed.get("job_name", "model")

    files = {}

    # ======================
    # STAGING
    # ======================
    stg = f"""
select
    {", ".join([f"{r['source']} as {r['target']}" for r in sttm])}
from {{ source('raw', '{model_name}') }}
"""

    files[f"models/staging/stg_{model_name}.sql"] = stg


    # ======================
    # INTERMEDIATE
    # ======================
    int_sql = f"""
select
    {", ".join([f"{r['transformation']} as {r['target']}" for r in sttm])}
from {{ ref('stg_{model_name}') }}
"""

    files[f"models/intermediate/int_{model_name}.sql"] = int_sql


    # ======================
    # MART
    # ======================
    mart = f"""
select *
from {{ ref('int_{model_name}') }}
"""

    files[f"models/marts/dim_{model_name}.sql"] = mart


    # ======================
    # SCHEMA
    # ======================
    schema = f"""
version: 2

models:
  - name: dim_{model_name}
"""

    files["models/schema.yml"] = schema

    return files
