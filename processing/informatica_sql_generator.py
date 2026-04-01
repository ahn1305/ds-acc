class InformaticaSQLGenerator:

    def __init__(self, graph):
        self.stages = graph.get("stages", {})
        self.execution_order = graph.get("execution_order", [])

    # =========================================
    # BUILD SOURCE (STAGING)
    # =========================================
    def build_staging(self, stage_name, stage):

        columns = [c["name"] for c in stage.get("outputs", [])] or ["*"]

        return f"""
{{{{ config(materialized='view') }}}}

SELECT
    {",\n    ".join(columns)}
FROM {{{{ source('raw', '{stage_name.lower()}') }}}}
""".strip()

    # =========================================
    # BUILD SELECT CLAUSE
    # =========================================
    def build_select(self, stage):

        select_lines = []

        for col in stage.get("outputs", []):
            name = col.get("name")
            derivation = col.get("derivation")

            if derivation and derivation != name:
                select_lines.append(f"{self.convert_expression(derivation)} AS {name}")
            else:
                select_lines.append(name)

        return ",\n    ".join(select_lines)

    # =========================================
    # BUILD FROM + JOIN LOGIC
    # =========================================
    def build_from_clause(self, stage):

        inputs = stage.get("inputs", [])

        if not inputs:
            return None

        base = inputs[0]

        sql = f"FROM {{{{ ref('{base.lower()}') }}}} t0"

        # Handle joins
        for i, inp in enumerate(inputs[1:], start=1):

            sql += f"""
LEFT JOIN {{{{ ref('{inp.lower()}') }}}} t{i}
ON 1=1"""

        return sql

    # =========================================
    # BUILD WHERE (FILTER)
    # =========================================
    def build_where(self, stage):

        condition = stage.get("filter_condition")

        if not condition:
            return ""

        return f"\nWHERE {self.convert_expression(condition)}"

    # =========================================
    # BUILD GROUP BY (AGGREGATOR)
    # =========================================
    def build_group_by(self, stage):

        if not stage.get("has_aggregation"):
            return ""

        group_cols = []

        for col in stage.get("outputs", []):
            expr = col.get("derivation", "")

            # if not aggregation → group by
            if not any(fn in (expr or "").lower() for fn in ["count(", "sum(", "avg(", "min(", "max("]):
                group_cols.append(col.get("name"))

        if not group_cols:
            return ""

        return f"\nGROUP BY {', '.join(group_cols)}"

    # =========================================
    # BUILD LOOKUP JOIN CONDITION
    # =========================================
    def build_lookup_condition(self, stage):

        condition = stage.get("lookup_condition")

        if not condition:
            return ""

        return f"\n-- Lookup Condition\n-- {condition}"

    # =========================================
    # BUILD TRANSFORM (SMART)
    # =========================================
    def build_transform(self, stage_name, stage):

        select_clause = self.build_select(stage)
        from_clause = self.build_from_clause(stage)

        if not from_clause:
            return None

        where_clause = self.build_where(stage)
        group_clause = self.build_group_by(stage)
        lookup_comment = self.build_lookup_condition(stage)

        return f"""
{{{{ config(materialized='table') }}}}

SELECT
    {select_clause}
{from_clause}
{where_clause}
{group_clause}

{lookup_comment}
""".strip()

    # =========================================
    # EXPRESSION → SQL (IMPROVED)
    # =========================================
    def convert_expression(self, expr):

        if not expr:
            return "NULL"

        sql = expr

        # Basic conversions
        sql = sql.replace("IIF(", "CASE WHEN ")
        sql = sql.replace("DECODE(", "CASE ")
        sql = sql.replace("SYSDATE", "CURRENT_TIMESTAMP")

        # Handle concatenation
        sql = sql.replace("||", " || ")

        # Ensure CASE END
        if "CASE" in sql and "END" not in sql:
            sql += " END"

        return sql

    # =========================================
    # RUN
    # =========================================
    def run(self):

        models = {
            "staging": {},
            "intermediate": {},
            "marts": {}
        }

        for stage_name in self.execution_order:

            stage = self.stages.get(stage_name, {})
            category = stage.get("category", "TRANSFORM")

            sql = None

            # -------------------------
            # SOURCE
            # -------------------------
            if category == "SOURCE":
                sql = self.build_staging(stage_name, stage)
                layer = "staging"

            # -------------------------
            # LOOKUP / JOIN / AGG
            # -------------------------
            elif category in ["LOOKUP", "JOIN", "AGGREGATE"]:
                sql = self.build_transform(stage_name, stage)
                layer = "intermediate"

            # -------------------------
            # TARGET / FINAL
            # -------------------------
            else:
                sql = self.build_transform(stage_name, stage)
                layer = "marts"

            if sql:
                models[layer][stage_name.lower()] = sql

        return models