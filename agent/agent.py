# # from google import genai
# # import json
# # from processing.lineage import LineageEngine
# # from processing.sql_generator import SnowflakeSQLGenerator


# # class DSXAgent:

# #     def __init__(self):
# #         self.client = genai.Client(api_key="YOUR_API_KEY")

# #     # ======================================================
# #     # 🚀 MAIN PIPELINE
# #     # ======================================================
# #     def run(self, parsed):

# #         print("\n🚀 ===== DSX AGENT STARTED =====\n")

# #         # --------------------------------------
# #         # STEP 1: LINEAGE
# #         # --------------------------------------
# #         lineage_engine = LineageEngine(parsed)
# #         lineage = lineage_engine.run()

# #         print("\n🔥 LINEAGE OUTPUT:\n")
# #         for l in lineage:
# #             print(l)

# #         # --------------------------------------
# #         # STEP 2: STTM
# #         # --------------------------------------
# #         sttm = self.generate_sttm_from_lineage(lineage)

# #         print("\n📊 STTM GENERATED:\n")
# #         print(json.dumps(sttm, indent=2))

# #         # --------------------------------------
# #         # STEP 3: LLM REFINEMENT (OPTIONAL)
# #         # --------------------------------------
# #         refined_sttm = self.refine_with_llm(sttm)

# #         print("\n🧠 REFINED STTM:\n")
# #         print(json.dumps(refined_sttm, indent=2))

# #         # --------------------------------------
# #         # STEP 4: SQL + DBT GENERATION
# #         # --------------------------------------
# #         sql_engine = SnowflakeSQLGenerator(parsed)
# #         dbt_models = sql_engine.run()

# #         print("\n❄️ DBT MODELS GENERATED:\n")
# #         print(json.dumps(dbt_models, indent=2))

# #         # --------------------------------------
# #         # STEP 5: RAW SQL (FLATTENED VIEW)
# #         # --------------------------------------
# #         snowflake_sql = self.flatten_sql(dbt_models)

# #         # --------------------------------------
# #         # STEP 6: DOCUMENTATION
# #         # --------------------------------------
# #         documentation = self.generate_documentation(parsed, refined_sttm)

# #         print("\n📄 DOCUMENTATION:\n")
# #         print(documentation)

# #         print("\n✅ ===== PIPELINE COMPLETED =====\n")

# #         return {
# #             "sttm": refined_sttm,
# #             "snowflake_sql": snowflake_sql,
# #             "dbt_sql": dbt_models,
# #             "documentation": documentation
# #         }

# #     # ======================================================
# #     # 📊 STTM GENERATION
# #     # ======================================================
# #     def generate_sttm_from_lineage(self, lineage):

# #         if not lineage:
# #             return [{
# #                 "source": "UNKNOWN",
# #                 "target": "UNKNOWN",
# #                 "transformation": "No lineage found"
# #             }]

# #         return lineage

# #     # ======================================================
# #     # 🧠 LLM REFINEMENT
# #     # ======================================================
# #     def refine_with_llm(self, sttm):

# #         if not sttm:
# #             return sttm

# #         prompt = f"""
# # You are a senior data engineer.

# # Improve transformation descriptions to be business-friendly.

# # STRICT JSON FORMAT:
# # [
# #   {{
# #     "source": "",
# #     "target": "",
# #     "transformation": ""
# #   }}
# # ]

# # Input:
# # {json.dumps(sttm)}
# # """

# #         try:
# #             response = self.client.models.generate_content(
# #                 model="gemini-3.1-flash-lite-preview",
# #                 contents=prompt
# #             )

# #             print("\n🧠 LLM RAW OUTPUT:\n")
# #             print(response.text)

# #             text = response.text.strip()

# #             if "```" in text:
# #                 text = text.split("```")[1]

# #             text = text.replace("json", "").strip()

# #             refined = json.loads(text)

# #             if isinstance(refined, list):
# #                 return refined

# #             return sttm

# #         except Exception as e:
# #             print("⚠️ LLM failed:", e)
# #             return sttm

# #     # ======================================================
# #     # ❄️ FLATTEN SQL (FOR UI DISPLAY)
# #     # ======================================================
# #     def flatten_sql(self, dbt_models):

# #         all_sql = []

# #         for layer, models in dbt_models.items():
# #             all_sql.append(f"\n-- ================= {layer.upper()} =================\n")

# #             for name, sql in models.items():
# #                 all_sql.append(f"\n-- MODEL: {name}\n")
# #                 all_sql.append(sql)

# #         return "\n".join(all_sql)

# #     # ======================================================
# #     # 📄 DOCUMENTATION GENERATOR
# #     # ======================================================
# #     def generate_documentation(self, parsed, sttm):

# #         doc = []

# #         doc.append(f"Job Name: {parsed.get('job_name')}\n")

# #         doc.append("\n📊 SOURCE → TARGET MAPPING:\n")

# #         for row in sttm:
# #             doc.append(
# #                 f"{row['source']} → {row['target']} | {row['transformation']}"
# #             )

# #         doc.append("\n🏗️ STAGES:\n")

# #         for stage, details in parsed["stages"].items():
# #             doc.append(f"{stage} ({details['type']})")

# #         return "\n".join(doc)


# import os
# import json
# from google import genai


# class DSXAgent:

#     def __init__(self):
#         self.client = genai.Client(api_key="AIzaSyBTCVoYJYpUbEoqgIqotXXJ2ah82N-r5wg")

#     # =========================================================
#     # MAIN ENTRY
#     # =========================================================
#     def run(self, parsed):

#         print("\n===== PARSED INPUT =====\n", parsed)

#         sttm = self.generate_sttm(parsed)

#         snowflake_sql, dbt_files = self.generate_dbt_sql(parsed, sttm)

#         documentation = self.generate_documentation(parsed, sttm)

#         return {
#             "sttm": sttm,
#             "snowflake_sql": snowflake_sql,
#             "dbt_files": dbt_files,
#             "documentation": documentation
#         }


#     # =========================================================
#     # STTM GENERATION (ROBUST)
#     # =========================================================
#     def generate_sttm(self, parsed):

#         sttm = []

#         try:
#             stages = parsed.get("stages", {})

#             # ✅ iterate correctly over dict
#             for stage_name, stage_data in stages.items():

#                 outputs = stage_data.get("outputs", [])

#                 for col in outputs:

#                     col_name = col.get("name")

#                     derivation = col.get("derivation")

#                     # -------------------------------
#                     # DETECT SOURCE
#                     # -------------------------------
#                     if derivation:
#                         source = derivation
#                     else:
#                         source = f"{stage_name}.{col_name}"

#                     # -------------------------------
#                     # INCOMPLETE LOGIC DETECTION
#                     # -------------------------------
#                     incomplete = False

#                     if not derivation:
#                         incomplete = True

#                     elif isinstance(derivation, str):
#                         d = derivation.strip()

#                         # broken DSX expressions
#                         if d.endswith("\\") or d.endswith(":") or d.endswith("="):
#                             incomplete = True

#                         if "Then \\" in d or "Else \\" in d:
#                             incomplete = True

#                         if d.lower() in ["", "null", "none"]:
#                             incomplete = True

#                     # -------------------------------
#                     # TRANSFORMATION CLEANUP
#                     # -------------------------------
#                     transformation = derivation if derivation else "DIRECT_MAPPING"

#                     # -------------------------------
#                     # APPEND
#                     # -------------------------------
#                     sttm.append({
#                         "source": source,
#                         "target": col_name,
#                         "transformation": transformation,
#                         "stage": stage_name,
#                         "incomplete": incomplete   # 🔥 UI highlight
#                     })

#         except Exception as e:
#             print("❌ STTM ERROR:", str(e))

#         # -------------------------------
#         # FALLBACK
#         # -------------------------------
#         if not sttm:
#             sttm = [{
#                 "source": "UNKNOWN",
#                 "target": "UNKNOWN",
#                 "transformation": "FAILED_PARSE",
#                 "stage": "UNKNOWN",
#                 "incomplete": True
#             }]

#         print("\n===== STTM SAMPLE =====\n", sttm[:5])

#         return sttm



#     # =========================================================
#     # SNOWFLAKE + DBT GENERATION (LLM POWERED)
#     # =========================================================
#     def generate_dbt_sql(self, parsed, sttm):

#         prompt = f"""
# You are a senior data engineer.

# Convert this ETL logic into Snowflake SQL using DBT standards.

# ### INPUT:

# Job Name:
# {parsed.get("job_name")}

# Stages:
# {parsed.get("stages")}

# STTM:
# {sttm}

# ---

# ### REQUIREMENTS:

# - Generate clean Snowflake SQL
# - Use CTEs for each stage
# - Handle joins properly
# - Apply transformations
# - Final SELECT should represent target table

# ---

# ### ALSO GENERATE:

# Return DBT project structure as JSON:

# {{
#   "models/staging/stg_x.sql": "...",
#   "models/marts/fact_x.sql": "...",
#   "models/schema.yml": "..."
# }}

# ---

# ### OUTPUT FORMAT:

# SQL:
# <sql_here>

# DBT_FILES:
# <json_here>
# """

#         response = self.client.models.generate_content(
#             model="gemini-3.1-flash-lite-preview",
#             contents=prompt
#         )

#         text = response.text

#         print("\n===== LLM SQL OUTPUT =====\n")
#         print(text)
#         print("\n=================================\n")

#         sql = ""
#         dbt_files = {}

#         try:
#             if "SQL:" in text:
#                 sql = text.split("SQL:")[1].split("DBT_FILES:")[0].strip()

#             if "DBT_FILES:" in text:
#                 json_part = text.split("DBT_FILES:")[1].strip()
#                 dbt_files = json.loads(json_part)

#         except Exception as e:
#             print("SQL PARSE ERROR:", str(e))

#         return sql, dbt_files

#     # =========================================================
#     # DOCUMENTATION (LLM)
#     # =========================================================
#     def generate_documentation(self, parsed, sttm):

#         prompt = f"""
# You are an expert ETL architect.

# Explain this pipeline in simple, human-readable format.

# ### INPUT:

# Job Name:
# {parsed.get("job_name")}

# Stages:
# {parsed.get("stages")}

# STTM:
# {sttm}

# ---

# ### OUTPUT:

# ## Overview
# ## Sources
# ## Transformations
# ## Business Rules
# ## Target
# ## Data Flow Summary

# Make it clean and understandable.
# """

#         response = self.client.models.generate_content(
#             model="gemini-3.1-flash-lite-preview",
#             contents=prompt
#         )

#         doc = response.text

#         print("\n===== DOCUMENTATION OUTPUT =====\n")
#         print(doc)
#         print("\n================================\n")

#         return doc


# # import os
# # import re
# # import json
# # from google import genai


# # class DSXAgent:

# #     def __init__(self):
# #         self.client = genai.Client(
# #             api_key="AIzaSyBTCVoYJYpUbEoqgIqotXXJ2ah82N-r5wg"
# #         )

# #     def run(self, parsed):

# #         print("\n===== PARSED INPUT =====\n", parsed)

# #         sttm = self.generate_sttm(parsed)
# #         snowflake_sql, dbt_files = self.generate_dbt_sql(parsed, sttm)
# #         documentation = self.generate_documentation(parsed, sttm)

# #         return {
# #             "sttm": sttm,
# #             "snowflake_sql": snowflake_sql,
# #             "dbt_files": dbt_files,
# #             "documentation": documentation
# #         }

# #     def generate_sttm(self, parsed):

# #         sttm = []

# #         try:
# #             for stage in parsed.get("stages", []):
# #                 for output in stage.get("outputs", []):
# #                     for col in output.get("columns", []):

# #                         source = col.get("derivation") or col.get("name")

# #                         sttm.append({
# #                             "source": source,
# #                             "target": col.get("name"),
# #                             "transformation": source
# #                         })

# #         except Exception as e:
# #             print("STTM ERROR:", str(e))

# #         if not sttm:
# #             sttm = [{
# #                 "source": "UNKNOWN",
# #                 "target": "UNKNOWN",
# #                 "transformation": "FAILED_PARSE"
# #             }]

# #         return sttm

# #     def extract_sql(self, text):

# #         match = re.search(r"```sql(.*?)```", text, re.DOTALL)
# #         if match:
# #             return match.group(1).strip()

# #         match = re.search(r"(WITH|SELECT).*", text, re.DOTALL)
# #         return match.group(0) if match else ""

# #     def extract_json(self, text):

# #         match = re.search(r"```json(.*?)```", text, re.DOTALL)

# #         if match:
# #             try:
# #                 return json.loads(match.group(1).strip())
# #             except Exception as e:
# #                 print("JSON ERROR:", e)

# #         return {}

# #     def generate_dbt_sql(self, parsed, sttm):

# #         prompt = f"""
# # You are a senior data engineer.

# # Convert this ETL logic into Snowflake SQL using DBT standards.

# # ### INPUT:

# # Job Name:
# # {parsed.get("job_name")}

# # Stages:
# # {parsed.get("stages")}

# # STTM:
# # {sttm}

# # ---

# # ### REQUIREMENTS:

# # - Generate clean Snowflake SQL
# # - Use CTEs for each stage
# # - Handle joins properly
# # - Apply transformations
# # - Final SELECT should represent target table

# # ---

# # ### ALSO GENERATE:

# # Return DBT project structure as JSON:

# # {{
# #   "models/staging/stg_x.sql": "...",
# #   "models/marts/fact_x.sql": "...",
# #   "models/schema.yml": "..."
# # }}

# # ---

# # ### OUTPUT FORMAT:

# # SQL:
# # <sql_here>

# # DBT_FILES:
# # <json_here>
# # """

# #         response = self.client.models.generate_content(
# #             model="gemini-3.1-flash-lite-preview",
# #             contents=prompt
# #         )

# #         text = response.text

# #         print("\n===== LLM SQL OUTPUT =====\n")
# #         print(text)
# #         print("\n=================================\n")

# #         sql = ""
# #         dbt_files = {}

# #         try:
# #             if "SQL:" in text:
# #                 sql = text.split("SQL:")[1].split("DBT_FILES:")[0].strip()

# #             if "DBT_FILES:" in text:
# #                 json_part = text.split("DBT_FILES:")[1].strip()
# #                 dbt_files = json.loads(json_part)

# #         except Exception as e:
# #             print("SQL PARSE ERROR:", str(e))

# #         return sql, dbt_files

# #     def generate_documentation(self, parsed, sttm):

# #         prompt = f'''
# # Explain this ETL pipeline in simple terms.

# # Job: {parsed.get("job_name")}
# # Stages: {parsed.get("stages")}

# # Sections:
# # Overview, Sources, Transformations, Target
# # '''

# #         res = self.client.models.generate_content(
# #             model="gemini-3.1-flash-lite-preview",
# #             contents=prompt
# #         )

# #         return res.text



# from google import genai
# import json

# from processing.lineage import LineageEngine
# from processing.sql_generator import SnowflakeSQLGenerator


# class DSXAgent:

#     def __init__(self):
#         self.client = genai.Client(api_key="AIzaSyBTCVoYJYpUbEoqgIqotXXJ2ah82N-r5wg")

#     # ======================================================
#     # 🚀 MAIN PIPELINE
#     # ======================================================
#     def run(self, parsed):

#         print("\n🚀 ===== DSX AGENT STARTED =====\n")

#         # --------------------------------------
#         # STEP 1: LINEAGE ENGINE
#         # --------------------------------------
#         lineage_engine = LineageEngine(parsed)
#         lineage = lineage_engine.run()

#         print("\n🔥 LINEAGE OUTPUT SAMPLE:\n")
#         print(lineage[:5] if lineage else "No lineage generated")

#         # --------------------------------------
#         # STEP 2: STTM GENERATION
#         # --------------------------------------
#         sttm = self.generate_sttm_from_lineage(lineage)

#         print("\n📊 STTM SAMPLE:\n")
#         print(json.dumps(sttm[:5], indent=2))

#         # --------------------------------------
#         # STEP 3: SQL + DBT GENERATION (NO LLM)
#         # --------------------------------------
#         sql_generator = SnowflakeSQLGenerator(parsed)
#         dbt_files = sql_generator.run()

#         print("\n❄️ DBT FILES GENERATED:\n")
#         print(list(dbt_files.keys()) if dbt_files else "No DBT files")

#         # --------------------------------------
#         # STEP 4: FLATTEN SQL FOR UI
#         # --------------------------------------
#         snowflake_sql = self.flatten_sql(dbt_files)

#         # --------------------------------------
#         # STEP 5: DOCUMENTATION (LLM ONLY HERE)
#         # --------------------------------------
#         documentation = self.generate_documentation(parsed, sttm, snowflake_sql)

#         dbt_files = self.generate_dbt_files(parsed)
#         snowflake_sql = self.flatten_sql(dbt_files)


#         print("\n📄 DOCUMENTATION GENERATED\n")

#         print("\n✅ ===== PIPELINE COMPLETED =====\n")

#         return {
#             "sttm": sttm,
#             "snowflake_sql": snowflake_sql,
#             "dbt_files": dbt_files,
#             "documentation": documentation
#         }

#     # ======================================================
#     # 📊 STTM GENERATION FROM LINEAGE (FIXED)
#     # ======================================================
#     def generate_sttm_from_lineage(self, lineage):

#         sttm = []

#         if not lineage:
#             return [{
#                 "source": "UNKNOWN",
#                 "target": "UNKNOWN",
#                 "transformation": "No lineage found",
#                 "stage": "UNKNOWN",
#                 "incomplete": True
#             }]

#         for col in lineage:

#             source = col.get("source", "")
#             target = col.get("target", "")
#             logic = col.get("logic")

#             # -----------------------------
#             # Determine transformation
#             # -----------------------------
#             if logic and isinstance(logic, str):
#                 transformation = logic.strip()
#             else:
#                 transformation = ""

#             # -----------------------------
#             # Detect DIRECT mapping properly
#             # -----------------------------
#             if not transformation or transformation.lower() in ["", "null", "none"]:
#                 if source == target:
#                     transformation = "DIRECT_MAPPING"
#                 else:
#                     transformation = f"{source} → {target}"

#             # -----------------------------
#             # Clean bad DSX artifacts
#             # -----------------------------
#             transformation = transformation.replace("\\", "").strip()

#             # -----------------------------
#             # Detect incomplete logic
#             # -----------------------------
#             incomplete = False

#             if isinstance(transformation, str):
#                 t = transformation.strip()

#                 if (
#                     t.endswith(("=", "Then", "Else", ":", "And", "Or")) or
#                     "Then" in t and "Else" not in t or
#                     "Else" in t and "Then" not in t or
#                     t.count("'") % 2 != 0  # unclosed quotes
#                 ):
#                     incomplete = True

#             # -----------------------------
#             # Normalize readable transformation
#             # -----------------------------
#             if transformation == "DIRECT_MAPPING":
#                 readable = f"{source} mapped directly"
#             else:
#                 readable = transformation

#             # -----------------------------
#             # Append row
#             # -----------------------------
#             sttm.append({
#                 "source": source,
#                 "target": target,
#                 "transformation": readable,
#                 "stage": col.get("stage", ""),
#                 "incomplete": incomplete
#             })

#         return sttm


#     # ======================================================
#     # ❄️ FLATTEN SQL (FOR UI DISPLAY)
#     # ======================================================
#     def flatten_sql(self, dbt_files):

#         if not dbt_files:
#             return ""

#         all_sql = []

#         for path, sql in dbt_files.items():

#             if path.endswith(".sql"):
#                 all_sql.append(f"\n-- FILE: {path}\n")
#                 all_sql.append(sql)

#         return "\n".join(all_sql)

#     # ======================================================
#     # 📄 DOCUMENTATION (LLM CLEAN VERSION)
#     # ======================================================
#     def generate_documentation(self, parsed, sttm, sql):

#         try:
#             prompt = f"""
# You are a senior data engineer.

# Explain this ETL pipeline in a clean, human-readable way.

# Make it simple and structured:

# 1. Overview
# 2. Source Systems
# 3. Transformations
# 4. Business Logic
# 5. Output

# Avoid technical noise. Make it readable for humans.

# STTM SAMPLE:
# {json.dumps(sttm[:10], indent=2)}

# SQL SAMPLE:
# {sql[:1500]}
# """

#             response = self.client.models.generate_content(
#                 model="gemini-3.1-flash-lite-preview",
#                 contents=prompt
#             )

#             text = response.text.strip()

#             return self.clean_llm_output(text)

#         except Exception as e:
#             print("⚠️ DOC GENERATION FAILED:", e)

#             # fallback
#             return self.generate_basic_doc(parsed, sttm)

#     # ======================================================
#     # 🧹 CLEAN LLM OUTPUT
#     # ======================================================
#     def clean_llm_output(self, text):

#         if "```" in text:
#             parts = text.split("```")
#             text = parts[1] if len(parts) > 1 else parts[0]

#         return text.replace("markdown", "").strip()

#     # ======================================================
#     # 📄 FALLBACK DOCUMENTATION
#     # ======================================================
#     def generate_basic_doc(self, parsed, sttm):

#         doc = []

#         doc.append(f"Job Name: {parsed.get('job_name')}\n")

#         doc.append("\nSOURCE → TARGET:\n")

#         for row in sttm[:20]:
#             doc.append(
#                 f"{row['source']} → {row['target']} | {row['transformation']}"
#             )

#         doc.append("\nSTAGES:\n")

#         for stage, details in parsed.get("stages", {}).items():
#             doc.append(f"{stage} ({details.get('type')})")

#         return "\n".join(doc)

#   # ======================================================
#     # ❄️ DBT FILE GENERATION
#     # ======================================================
#     def generate_dbt_files(self, parsed):
#         """
#         Returns a dictionary representing a DBT project structure
#         with SQL files and project.yml
#         """
#         dbt_files = {}

#         # Example: models folder with one file per table
#         for table in parsed.get("tables", []):
#             table_name = table.get("target_table", "unknown_table")
#             file_path = f"models/{table_name}.sql"

#             # SQL content: could be complex based on transformations
#             sql_content = f"-- DBT model for {table_name}\n"
#             for col in table.get("columns", []):
#                 sql_content += f"SELECT {col['source']} AS {col['target']}\n"
#             sql_content += f"FROM {table.get('source_table', 'unknown_source')};\n"

#             dbt_files[file_path] = sql_content

#         # Minimal dbt_project.yml
#         dbt_files["dbt_project.yml"] = """
# name: 'dsx_project'
# version: '1.0'
# config-version: 2
# profile: 'default'
# model-paths: ['models']
# """


#         return dbt_files


from google import genai
import json
import os

from processing.lineage import LineageEngine
from processing.sql_generator import SnowflakeSQLGenerator

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
        # STEP 5: DOCUMENTATION (LLM ONLY HERE)
        # --------------------------------------
        documentation = self.generate_documentation(parsed, sttm, snowflake_sql)

        print("\n📄 DOCUMENTATION GENERATED\n")
        print("\n✅ ===== PIPELINE COMPLETED =====\n")

        return {
            "sttm": sttm,
            "snowflake_sql": snowflake_sql,
            "dbt_files": dbt_files,
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
        schema_yml = self.generate_schema_yml(raw_models)

        dbt_project["models/sources.yml"] = sources_yml
        dbt_project["models/schema.yml"] = schema_yml

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
        

    
    def generate_schema_yml(self, raw_models):

        models_yaml = []

        for layer, models in raw_models.items():

            for model_name in models.keys():

                columns = []

                # 🔥 smart default tests
                columns.append({
                    "name": "id",
                    "tests": ["not_null", "unique"]
                })

                columns.append({
                    "name": "created_at",
                    "tests": ["not_null"]
                })

                models_yaml.append({
                    "name": model_name,
                    "columns": columns
                })

        final = {
            "version": 2,
            "models": models_yaml
        }

        return self.to_yaml(final)
    
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
            return self.clean_llm_output(text)

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
