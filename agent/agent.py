from google import genai
import json,re,base64
import os

from processing.lineage import LineageEngine
from processing.sql_generator import SnowflakeSQLGenerator


from processing.informatica_parser import InformaticaParser
from processing.informatica_graph_builder import InformaticaGraphBuilder
from processing.informatica_lineage_engine import InformaticaLineageEngine
from processing.informatica_sttm_generator import InformaticaSTTMGenerator
from processing.informatica_sql_generator import InformaticaSQLGenerator
from processing.informatica_documentation_generator import InformaticaDocumentationGenerator


class DSXAgent:

    def __init__(self):
        self.client = genai.Client(api_key="")

    # ======================================================
    # 🚀 MAIN PIPELINE
    # ======================================================
    def run(self, parsed):

        print("\n🚀 ===== DSX AGENT STARTED =====\n")

        # --------------------------------------
        # STEP 1: LINEAGE ENGINE
        # --------------------------------------
        lineage_engine = LineageEngine(parsed)
        lineage = lineage_engine.run()

        print("\n🔥 LINEAGE OUTPUT SAMPLE:\n")
        print(lineage[:5] if lineage else "No lineage generated")

        # --------------------------------------
        # STEP 2: STTM GENERATION
        # --------------------------------------
        sttm = self.generate_sttm_from_lineage(lineage)

        print("\n📊 STTM SAMPLE:\n")
        print(json.dumps(sttm[:5], indent=2))

        # --------------------------------------
        # STEP 3: SQL + DBT GENERATION
        # --------------------------------------
        sql_generator = SnowflakeSQLGenerator(parsed)
        dbt_files = self.generate_dbt_project(sql_generator,parsed)

        print("\n❄️ DBT FILES GENERATED:\n")
        print(list(dbt_files.keys()) if dbt_files else "No DBT files")

        # --------------------------------------
        # STEP 4: FLATTEN SQL FOR UI
        # --------------------------------------
        snowflake_sql = self.flatten_sql(dbt_files)

        # --------------------------------------
        # STEP 5: Data Modeller
        # --------------------------------------

        data_model = self.generate_data_model(snowflake_sql)

        # --------------------------------------
        # STEP 6: Data Modeller - ER Diagram
        # --------------------------------------

        er_diagram = self.generate_er_from_ddl_data_vault(json.loads(data_model))

        # --------------------------------------
        # STEP 7: DOCUMENTATION (LLM ONLY HERE)
        # --------------------------------------
        documentation = self.generate_documentation(parsed, sttm, snowflake_sql)

        print("\n📄 DOCUMENTATION GENERATED\n")
        print("\n✅ ===== PIPELINE COMPLETED =====\n")

        return {
            "sttm": sttm,
            "snowflake_sql": snowflake_sql,
            "dbt_files": dbt_files,
            "data_model": data_model,
            "er_diagram": er_diagram,
            "documentation": documentation
        }

    # ======================================================
    # 📊 STTM GENERATION FROM LINEAGE
    # ======================================================
    def generate_sttm_from_lineage(self, lineage):

        sttm = []

        if not lineage:
            return [{
                "source": "UNKNOWN",
                "target": "UNKNOWN",
                "transformation": "No lineage found",
                "stage": "UNKNOWN",
                "type": "UNKNOWN",
                "confidence": 0,
                "incomplete": True
            }]

        for col in lineage:

            source = col.get("source", "")
            target = col.get("target", "")
            logic = col.get("logic")
            stage = col.get("stage", "UNKNOWN")

            # -----------------------------
            # CLEAN TRANSFORMATION
            # -----------------------------
            transformation = logic.strip() if isinstance(logic, str) else ""
            transformation = transformation.replace("\\", "").strip()

            # -----------------------------
            # DETERMINE TYPE
            # -----------------------------
            if not source:
                t_type = "SYSTEM"

            elif "," in source:
                t_type = "DERIVED"

            elif "lookup" in stage.lower():
                t_type = "LOOKUP"

            elif "join" in stage.lower():
                t_type = "JOIN"

            elif source == target and not transformation:
                t_type = "DIRECT"

            else:
                t_type = "DERIVED"

            # -----------------------------
            # DEFAULT TRANSFORMATION
            # -----------------------------
            if not transformation or transformation.lower() in ["", "null", "none"]:

                if t_type == "DIRECT":
                    transformation = f"{source} mapped directly"

                elif t_type == "SYSTEM":
                    transformation = f"Generated field ({target})"

                else:
                    transformation = f"{source} → {target}"

            # -----------------------------
            # 🔥 INCOMPLETE DETECTION
            # -----------------------------
            incomplete = False
            t = transformation.lower()

            if (
                "\\" in (logic or "") or
                "then" in t and "else" not in t or
                "else" in t and "then" not in t or
                t.endswith(("=", "then", "else", "and", "or")) or
                t.count("'") % 2 != 0 or
                ("if" in t and "then" not in t) or
                ("," in source and t_type != "DERIVED") or
                (t_type != "DIRECT" and "mapped directly" in t)
            ):
                incomplete = True

            # -----------------------------
            # 🎯 CONFIDENCE SCORE
            # -----------------------------
            confidence = 100

            if incomplete:
                confidence -= 40

            if t_type == "DERIVED":
                confidence -= 10

            if t_type == "SYSTEM":
                confidence -= 20

            if "," in source:
                confidence -= 10

            confidence = max(confidence, 0)

            # -----------------------------
            # FINAL ROW
            # -----------------------------
            sttm.append({
                "source": source or "SYSTEM",
                "target": target,
                "transformation": transformation,
                "stage": stage,
                "type": t_type,
                "confidence": confidence,
                "incomplete": incomplete
            })

        return sttm

    # ======================================================
    # ❄️ FLATTEN SQL (FOR UI DISPLAY)
    # ======================================================
    def flatten_sql(self, dbt_files):

        if not dbt_files:
            return "-- No SQL generated"

        all_sql = []

        for path, sql in dbt_files.items():

            if path.endswith(".sql") and sql:

                all_sql.append(f"\n-- FILE: {path}\n{sql}")

        return "\n".join(all_sql) if all_sql else "-- No SQL generated"
    # ======================================================
    # 📂 GENERATE DBT PROJECT STRUCTURE
    # ======================================================
    def generate_dbt_project(self, sql_generator, parsed):

        raw_models = sql_generator.run()

        dbt_project = {}

        # -----------------------------------
        # dbt_project.yml
        # -----------------------------------
        dbt_project["dbt_project.yml"] = """
    name: dsx_project
    version: '1.0'
    config-version: 2
    profile: default

    model-paths: ["models"]

    models:
    dsx_project:
        staging:
        +materialized: view
        intermediate:
        +materialized: table
        marts:
        +materialized: table
    """

        # -----------------------------------
        # BUILD MODELS (.sql files)
        # -----------------------------------
        for layer, models in raw_models.items():

            for model_name, sql in models.items():

                if not sql:
                    continue

                file_path = f"models/{layer}/{model_name}.sql"
                dbt_project[file_path] = sql

        # -----------------------------------
        # ADD SOURCES + TESTS
        # -----------------------------------
        sources_yml = self.generate_sources_yml(parsed)
        # schema_yml = self.generate_schema_yml(raw_models)

        dbt_project["models/sources.yml"] = sources_yml
        # dbt_project["models/schema.yml"] = schema_yml

        return dbt_project
    
    def generate_sources_yml(self, parsed):

        stages = parsed.get("stages", {})
        links = parsed.get("links", [])

        # ----------------------------------------
        # BUILD INPUT MAP (who feeds whom)
        # ----------------------------------------
        incoming_map = {stage: [] for stage in stages.keys()}

        for link in links:
            target = link.get("to")
            source = link.get("from")

            if target in incoming_map:
                incoming_map[target].append(source)

        # ----------------------------------------
        # DETECT SOURCE STAGES
        # ----------------------------------------
        source_stages = []

        for stage_name, stage in stages.items():

            stage_type = stage.get("type", "")
            inputs = incoming_map.get(stage_name, [])

            # 🔥 RULE 1: no upstream → source
            is_source = len(inputs) == 0

            # 🔥 RULE 2: fallback type-based
            if stage_type.lower() in [
                "pxdataset",
                "pxsequentialfile",
                "pxodbcconnector",
                "pxoracleconnector",
                "pxjdbcconnector"
            ]:
                is_source = True

            # 🔥 RULE 3: naming heuristic
            if any(x in stage_name.lower() for x in ["input", "src", "source", "file"]):
                is_source = True

            if is_source:
                source_stages.append((stage_name, stage))

        # ----------------------------------------
        # BUILD TABLES
        # ----------------------------------------
        tables = []

        for stage_name, stage in source_stages:

            columns = stage.get("outputs", [])

            col_list = []

            for col in columns:
                col_list.append({
                    "name": col.get("name"),
                    "description": ""
                })

            tables.append({
                "name": stage_name.lower(),
                "identifier": stage_name,
                "columns": col_list
            })

        # ----------------------------------------
        # FINAL YAML
        # ----------------------------------------
        final = {
            "version": 2,
            "sources": [
                {
                    "name": "raw",
                    "database": "RAW_DB",
                    "schema": "PUBLIC",
                    "tables": tables
                }
            ]
        }

        return self.to_yaml(final)
    
    def generate_data_model(self,sql):
        prompt = f"""
            You are an expert Data Vault architect.

    Convert the SQL below into Data Vault structures.

    Steps:
    1. Identify business keys → HUB tables
    2. Identify relationships → LINK tables
    3. Identify attributes → SATELLITE tables

    Guidelines:
    - HUB: only business keys + hash key
    - LINK: connects HUB keys
    - SAT: descriptive columns + load date + hash diff

    Return ONLY valid JSON:

    {{
    "hubs": {{
        "hub_customer": "SQL"
    }},
    "links": {{
        "link_customer_order": "SQL"
    }},
    "satellites": {{
        "sat_customer": "SQL"
    }}
    }}

    SQL:
    {sql}

    """
        response = self.client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=prompt
            )
        
        return response.text.strip()
    
    def generate_er_from_ddl_data_vault(self,data_vault: dict) -> str:
        """
        Converts your DDL-based Data Vault JSON into Mermaid ER diagram
        """

        lines = ["erDiagram"]

        hub_keys = {}   # hk → hub_name

        # -----------------------
        # Hubs
        # -----------------------
        for hub_name, ddl in data_vault.get("hubs", {}).items():
            cols, pks = self.parse_columns_from_ddl(ddl)

            lines.append(f"  {hub_name.upper()} {{")
            for col in cols:
                tag = "PK" if col in pks else ""
                lines.append(f"    string {col} {tag}".strip())
            lines.append("  }")

            # Store hash key mapping
            for pk in pks:
                hub_keys[pk] = hub_name

        # -----------------------
        # Links
        # -----------------------
        link_fks = {}

        for link_name, ddl in data_vault.get("links", {}).items():
            cols, pks = self.parse_columns_from_ddl(ddl)

            lines.append(f"  {link_name.upper()} {{")
            for col in cols:
                tag = "PK" if col in pks else "FK" if col.startswith("hk_") else ""
                lines.append(f"    string {col} {tag}".strip())
            lines.append("  }")

            link_fks[link_name] = [c for c in cols if c.startswith("hk_")]

        # -----------------------
        # Satellites
        # -----------------------
        for sat_name, ddl in data_vault.get("satellites", {}).items():
            cols, pks = self.parse_columns_from_ddl(ddl)

            lines.append(f"  {sat_name.upper()} {{")
            for col in cols:
                tag = "PK" if col in pks else "FK" if col.startswith("hk_") else ""
                lines.append(f"    string {col} {tag}".strip())
            lines.append("  }")

        # -----------------------
        # Relationships
        # -----------------------

        # Hub ↔ Link
        for link_name, fks in link_fks.items():
            for fk in fks:
                if fk in hub_keys:
                    hub_name = hub_keys[fk]
                    lines.append(
                        f"  {hub_name.upper()} ||--o{{ {link_name.upper()} : links"
                    )

        # Satellite ↔ Hub/Link
        for sat_name, ddl in data_vault.get("satellites", {}).items():
            cols, _ = self.parse_columns_from_ddl(ddl)

            for col in cols:
                if col in hub_keys:
                    lines.append(
                        f"  {hub_keys[col].upper()} ||--o{{ {sat_name.upper()} : has"
                    )
                elif col.startswith("hk_"):
                    # assume satellite linked to link
                    for link_name in data_vault.get("links", {}):
                        lines.append(
                            f"  {link_name.upper()} ||--o{{ {sat_name.upper()} : has"
                        )
                    break

        diagram_text = "\n".join(lines)

        prompt = f"""
        You are a senior data architect and data modeling expert.

        Your task is to enhance and refine a Mermaid ER diagram generated from a Data Vault model.

        INPUT:
        You will receive a Mermaid ER diagram that contains:
        - Hubs, Links, Satellites
        - Technical column names (e.g., hk_account_id, load_date)
        - System-oriented structure

        OBJECTIVE:
        Transform this into a clean, business-friendly ER diagram.

        RULES:
        1. Keep valid Mermaid ER syntax (erDiagram)
        2. DO NOT add explanations — output ONLY Mermaid code
        3. Improve naming:
        - Remove prefixes like "hub_", "sat_", "link_"
        - Convert names to business entities (e.g., ACCOUNT, TRANSACTION)
        4. Simplify columns:
        - Replace hash keys (hk_*) with business keys (e.g., account_id)
        - Remove technical fields like load_date, record_source, hash_diff
        5. Improve relationships:
        - Add meaningful relationship names (e.g., "makes", "belongs_to", "has_risk")
        - Ensure correct cardinality (||--o{{, etc.)
        6. Merge satellites into their parent entities where appropriate
        7. Keep diagram clean and minimal — focus on business understanding
        8. Preserve all important business attributes

        OPTIONAL IMPROVEMENTS:
        - Group related attributes logically
        - Rename vague fields into meaningful business terms

        INPUT DIAGRAM:
        {diagram_text}

        OUTPUT:
        Return ONLY the improved Mermaid ER diagram.
        """

        response = self.client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=prompt
            )
        text = response.text.strip()
        graph_bytes = text.encode("utf8")
        base64_bytes = base64.b64encode(graph_bytes)
        base64_string = base64_bytes.decode("ascii")
        return f"https://mermaid.ink/img/{base64_string}"
        # return text

    
    def parse_columns_from_ddl(self,ddl: str):
        """
        Extract column definitions from CREATE TABLE SQL
        """
        inside = ddl[ddl.find("(")+1: ddl.rfind(")")]
        parts = [p.strip() for p in inside.split(",")]

        columns = []
        pk_columns = []

        for part in parts:
            if part.upper().startswith("PRIMARY KEY"):
                pk_match = re.findall(r"\((.*?)\)", part)
                if pk_match:
                    pk_columns.extend([c.strip() for c in pk_match[0].split(",")])
                continue

            tokens = part.split()
            col_name = tokens[0]

            if "PRIMARY" in part.upper():
                pk_columns.append(col_name)

            columns.append(col_name)

        return columns, pk_columns
            

    
   
    
    def to_yaml(self, data):

        try:
            import yaml
            return yaml.dump(data, sort_keys=False)
        except:
            # fallback if PyYAML not installed
            return json.dumps(data, indent=2)
                


    # ======================================================
    # 📄 DOCUMENTATION GENERATION (LLM)
    # ======================================================
    def generate_documentation(self, parsed, sttm, sql):

        try:
            prompt = f"""
You are a senior data engineer.

Explain this ETL pipeline in a clean, human-readable way.

Make it simple and structured:

1. Overview
2. Source Systems
3. Transformations
4. Business Logic
5. Output

STTM SAMPLE:
{json.dumps(sttm[:10], indent=2)}

SQL SAMPLE:
{sql[:1500]}
"""
            response = self.client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=prompt
            )
            text = response.text.strip()
            return text

        except Exception as e:
            print("⚠️ DOC GENERATION FAILED:", e)
            return self.generate_basic_doc(parsed, sttm)
        



    # ======================================================
    # 🧹 CLEAN LLM OUTPUT
    # ======================================================
    def clean_llm_output(self, text):
        if "```" in text:
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else parts[0]
        return text.replace("markdown", "").strip()

    # ======================================================
    # 📄 FALLBACK DOCUMENTATION
    # ======================================================
    def generate_basic_doc(self, parsed, sttm):
        doc = [f"Job Name: {parsed.get('job_name')}\n", "\nSOURCE → TARGET:\n"]
        for row in sttm[:20]:
            doc.append(f"{row['source']} → {row['target']} | {row['transformation']}")
        doc.append("\nSTAGES:\n")
        for stage, details in parsed.get("stages", {}).items():
            doc.append(f"{stage} ({details.get('type')})")
        return "\n".join(doc)






class InformaticaPipeline:

    def __init__(self, llm_client=None, debug=True):
        self.llm_client = llm_client
        self.debug = debug

    # ======================================================
    # 🚀 MAIN PIPELINE
    # ======================================================
    def run(self, file_path):

        self._log("\n🚀 ===== INFORMATICA PIPELINE STARTED =====\n")

        try:
            # --------------------------------------
            # STEP 1: PARSE XML
            # --------------------------------------
            parser = InformaticaParser()
            parsed = parser.parse(file_path)

            self._log(f"\n📥 PARSED JOB: {parsed.get('job_name')}")

            if not parsed.get("stages"):
                raise ValueError("❌ No stages parsed")

            # --------------------------------------
            # STEP 2: BUILD GRAPH
            # --------------------------------------
            graph_builder = InformaticaGraphBuilder(parsed)
            graph = graph_builder.run()

            self._log("\n🔗 GRAPH BUILT")
            self._log(f"Stages: {len(graph.get('stages', {}))}")
            self._log(f"Links: {len(graph.get('links', []))}")

            # --------------------------------------
            # STEP 3: LINEAGE
            # --------------------------------------
            lineage_engine = InformaticaLineageEngine(graph)
            lineage = lineage_engine.run()

            self._log("\n🔥 LINEAGE GENERATED")
            self._log(lineage[:3] if lineage else "No lineage")

            # --------------------------------------
            # STEP 4: STTM
            # --------------------------------------
            sttm_gen = InformaticaSTTMGenerator(lineage, graph)
            sttm = sttm_gen.run()

            self._log("\n📊 STTM GENERATED")
            self._log(sttm[:3] if sttm else "No STTM")

            # --------------------------------------
            # STEP 5: SQL GENERATION
            # --------------------------------------
            sql_gen = InformaticaSQLGenerator(graph)
            models = sql_gen.run()

            self._log("\n❄️ SQL MODELS GENERATED")
            for layer, m in models.items():
                self._log(f"{layer}: {list(m.keys())}")

            # --------------------------------------
            # STEP 6: FLATTEN SQL (UI PURPOSE)
            # --------------------------------------
            flat_sql = self.flatten_sql(models)

            # --------------------------------------
            # STEP 7: DOCUMENTATION
            # --------------------------------------
            doc_gen = InformaticaDocumentationGenerator(
                parsed, sttm, models, self.llm_client
            )
            documentation = doc_gen.run()

            self._log("\n📄 DOCUMENTATION GENERATED")
            self._log("\n✅ ===== PIPELINE COMPLETED =====\n")

            # --------------------------------------
            # FINAL RESPONSE (🔥 ENHANCED)
            # --------------------------------------
            return {
                "job_name": parsed.get("job_name"),

                # Core outputs
                "parsed": parsed,
                "graph": graph,

                # Lineage
                "lineage": lineage,
                "lineage_graph": graph.get("lineage_graph"),
                "column_map": graph.get("column_map"),

                # STTM
                "sttm": sttm,

                # SQL
                "sql_models": models,
                "snowflake_sql": flat_sql,

                # Execution
                "execution_order": graph.get("execution_order"),

                # Metadata
                "stage_summary": self.build_stage_summary(graph),

                # Documentation
                "documentation": documentation
            }

        except Exception as e:
            self._log(f"\n❌ PIPELINE FAILED: {str(e)}")

            return {
                "error": str(e),
                "job_name": parsed.get("job_name") if 'parsed' in locals() else None
            }

    # ======================================================
    # 📊 STAGE SUMMARY (🔥 NEW)
    # ======================================================
    def build_stage_summary(self, graph):

        summary = []

        for stage_name, stage in graph.get("stages", {}).items():

            summary.append({
                "stage": stage_name,
                "category": stage.get("category"),
                "inputs": stage.get("inputs"),
                "outputs": [c["name"] for c in stage.get("outputs", [])],
                "has_filter": stage.get("has_filter"),
                "has_lookup": stage.get("has_lookup"),
                "has_aggregation": stage.get("has_aggregation")
            })

        return summary

    # ======================================================
    # ❄️ FLATTEN SQL (UI PURPOSE)
    # ======================================================
    def flatten_sql(self, models):

        if not models:
            return "-- No SQL generated"

        all_sql = []

        for layer, layer_models in models.items():

            for model_name, sql in layer_models.items():

                if not sql:
                    continue

                all_sql.append(
                    f"\n-- LAYER: {layer.upper()} | MODEL: {model_name}\n{sql}"
                )

        return "\n".join(all_sql) if all_sql else "-- No SQL generated"

    # ======================================================
    # 🧠 LOGGER (DEBUG CONTROL)
    # ======================================================
    def _log(self, msg):

        if self.debug:
            print(msg)



 # def generate_schema_yml(self, raw_models):

    #     models_yaml = []

    #     for layer, models in raw_models.items():

    #         for model_name in models.keys():

    #             columns = []

    #             # 🔥 smart default tests
    #             columns.append({
    #                 "name": "id",
    #                 "tests": ["not_null", "unique"]
    #             })

    #             columns.append({
    #                 "name": "created_at",
    #                 "tests": ["not_null"]
    #             })

    #             models_yaml.append({
    #                 "name": model_name,
    #                 "columns": columns
    #             })

    #     final = {
    #         "version": 2,
    #         "models": models_yaml
    #     }

    #     return self.to_yaml(final)


            # try:
        #     return json.loads(response.json)
        # except:
        #     # extract JSON from text
        #     match = re.search(r'\{.*\}', response.text.strip(), re.DOTALL)
        #     if match:
        #         return json.loads(match.group(0))

        # return {
        #     "hubs": {},
        #     "links": {},
        #     "satellites": {}
        # }
    
