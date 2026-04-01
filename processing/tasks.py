# # # from celery import shared_task
# # # import os
# # # import zipfile

# # # from ingestion.models import DSXFile
# # # from .parser import DSXParser
# # # from agent.agent import DSXAgent
# # # from processing.sttm_excel import generate_sttm_excel

# # # from channels.layers import get_channel_layer
# # # from asgiref.sync import async_to_sync


# # # # =========================
# # # # SAVE OUTPUT SQL FILE
# # # # =========================
# # # def save_output(file_obj, result):
# # #     base = f"media/output/{file_obj.batch.id}/"
# # #     os.makedirs(base, exist_ok=True)

# # #     with open(base + f"{file_obj.id}.sql", "w") as f:
# # #         f.write(result.get("snowflake_sql", ""))


# # # # =========================
# # # # ZIP GENERATOR
# # # # =========================
# # # def zip_batch(batch_id):
# # #     folder = f"media/output/{batch_id}"
# # #     zip_path = f"media/output/{batch_id}.zip"

# # #     with zipfile.ZipFile(zip_path, 'w') as z:
# # #         for root, _, files in os.walk(folder):
# # #             for file in files:
# # #                 z.write(os.path.join(root, file))


# # # # =========================
# # # # WEBSOCKET UPDATE
# # # # =========================
# # # def send_ws_update(batch_id, data):
# # #     channel_layer = get_channel_layer()

# # #     async_to_sync(channel_layer.group_send)(
# # #         f"batch_{batch_id}",
# # #         {
# # #             "type": "send_update",
# # #             "data": data
# # #         }
# # #     )


# # # # =========================
# # # # PROCESS BATCH
# # # # =========================
# # # @shared_task
# # # def process_batch(batch_id):
# # #     from processing.tasks import process_file  # ✅ important

# # #     files = DSXFile.objects.filter(batch_id=batch_id)

# # #     for f in files:
# # #         process_file.delay(f.id)


# # # @shared_task(bind=True, max_retries=2)
# # # def process_file(self, file_id):
# # #     try:
# # #         file_obj = DSXFile.objects.get(id=file_id)

# # #         file_obj.status = "PROCESSING"
# # #         file_obj.save()

# # #         send_ws_update(file_obj.batch.id, {
# # #             "file_id": file_obj.id,
# # #             "status": "PROCESSING"
# # #         })

# # #         parser = DSXParser()
# # #         parsed = parser.parse(file_obj.file.path)

# # #         print("PARSED:", parsed)

# # #         agent = DSXAgent()
# # #         result = agent.run(parsed)

# # #         excel_path = generate_sttm_excel(file_obj, result["sttm"])

# # #         file_obj.sttm_json = result["sttm"]
# # #         file_obj.snowflake_sql = result["snowflake_sql"]
# # #         file_obj.dbt_sql = result["dbt_sql"]
# # #         file_obj.documentation = result["documentation"]
# # #         file_obj.sttm_excel = excel_path
# # #         file_obj.status = "DONE"
# # #         file_obj.save()

# # #         send_ws_update(file_obj.batch.id, {
# # #             "file_id": file_obj.id,
# # #             "status": "DONE",
# # #             "sttm": result["sttm"],
# # #             "snowflake_sql": result["snowflake_sql"],
# # #             "dbt_sql": result["dbt_sql"],
# # #             "documentation": result["documentation"]
# # #         })

# # #     except Exception as e:
# # #         file_obj.status = "FAILED"
# # #         file_obj.save()

# # #         send_ws_update(file_obj.batch.id, {
# # #             "file_id": file_obj.id,
# # #             "status": "FAILED",
# # #             "error": str(e)
# # #         })

# # #         raise self.retry(exc=e, countdown=10)


# # from celery import shared_task
# # import os
# # import zipfile
# # import traceback

# # from ingestion.models import DSXFile
# # from .parser import DSXParser
# # from agent.agent import DSXAgent
# # from processing.sttm_excel import generate_sttm_excel

# # from channels.layers import get_channel_layer
# # from asgiref.sync import async_to_sync


# # # =========================================================
# # # WEBSOCKET
# # # =========================================================
# # def send_ws_update(batch_id, data):
# #     channel_layer = get_channel_layer()

# #     async_to_sync(channel_layer.group_send)(
# #         f"batch_{batch_id}",
# #         {
# #             "type": "send_update",
# #             "data": data
# #         }
# #     )


# # # =========================================================
# # # SAVE DBT FILES
# # # =========================================================
# # def save_dbt_files(file_obj, dbt_files):
# #     base = f"media/dbt/{file_obj.batch.id}/{file_obj.id}/"

# #     for path, content in dbt_files.items():
# #         full_path = os.path.join(base, path)
# #         os.makedirs(os.path.dirname(full_path), exist_ok=True)

# #         with open(full_path, "w", encoding="utf-8") as f:
# #             f.write(content)


# # def zip_dbt_project(file_obj):
# #     folder = f"media/dbt/{file_obj.batch.id}/{file_obj.id}"
# #     zip_path = f"media/dbt/{file_obj.batch.id}/{file_obj.id}.zip"

# #     with zipfile.ZipFile(zip_path, 'w') as z:
# #         for root, _, files in os.walk(folder):
# #             for file in files:
# #                 full = os.path.join(root, file)
# #                 z.write(full, arcname=os.path.relpath(full, folder))

# #     return zip_path


# # # =========================================================
# # # PROCESS BATCH
# # # =========================================================
# # @shared_task
# # def process_batch(batch_id):
# #     from processing.tasks import process_file

# #     files = DSXFile.objects.filter(batch_id=batch_id)

# #     for f in files:
# #         process_file.delay(f.id)


# # # =========================================================
# # # PROCESS FILE
# # # =========================================================
# # @shared_task(bind=True, max_retries=2)
# # def process_file(self, file_id):

# #     file_obj = None

# #     try:
# #         file_obj = DSXFile.objects.get(id=file_id)

# #         # -------------------------------
# #         # START
# #         # -------------------------------
# #         file_obj.status = "PROCESSING"
# #         file_obj.save()

# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "status": "PROCESSING",
# #             "step": "START"
# #         })

# #         # -------------------------------
# #         # PARSING
# #         # -------------------------------
# #         parser = DSXParser()
# #         parsed = parser.parse(file_obj.file.path)

# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "step": "PARSING"
# #         })

# #         print("\n===== PARSED =====\n", parsed)

# #         # -------------------------------
# #         # AI PROCESSING
# #         # -------------------------------
# #         agent = DSXAgent()
# #         result = agent.run(parsed)

# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "step": "AI_PROCESSING"
# #         })

# #         # -------------------------------
# #         # STTM
# #         # -------------------------------
# #         sttm = result.get("sttm", [])

# #         excel_path = generate_sttm_excel(file_obj, sttm)

# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "step": "STTM_GENERATED",
# #             "sttm": sttm
# #         })

# #         # -------------------------------
# #         # SQL (DBT SQL)
# #         # -------------------------------
# #         sql = result.get("snowflake_sql", "")

# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "step": "SQL_GENERATED",
# #             "snowflake_sql": sql
# #         })

# #         # -------------------------------
# #         # DBT FILES
# #         # -------------------------------
# #         dbt_files = result.get("dbt_files", {})

# #         dbt_zip = None

# #         if dbt_files:
# #             save_dbt_files(file_obj, dbt_files)
# #             dbt_zip = zip_dbt_project(file_obj)

# #         # -------------------------------
# #         # DOCUMENTATION
# #         # -------------------------------
# #         documentation = result.get("documentation", "")

# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "step": "DOC_GENERATED",
# #             "documentation": documentation
# #         })

# #         # -------------------------------
# #         # SAVE DB
# #         # -------------------------------
# #         file_obj.sttm_json = sttm
# #         file_obj.snowflake_sql = sql
# #         file_obj.documentation = documentation
# #         file_obj.sttm_excel = excel_path

# #         if dbt_zip:
# #             file_obj.dbt_zip = dbt_zip

# #         file_obj.status = "DONE"
# #         file_obj.save()

# #         # -------------------------------
# #         # FINAL UPDATE
# #         # -------------------------------
# #         send_ws_update(file_obj.batch.id, {
# #             "file_id": file_obj.id,
# #             "status": "DONE",
# #             "step": "DONE",
# #             "sttm": sttm,
# #             "snowflake_sql": sql,
# #             "documentation": documentation,
# #             "dbt_zip": dbt_zip
# #         })

# #     except Exception as e:

# #         error_msg = str(e)
# #         print("\n===== ERROR =====\n", traceback.format_exc())

# #         if file_obj:
# #             file_obj.status = "FAILED"
# #             file_obj.save()

# #             send_ws_update(file_obj.batch.id, {
# #                 "file_id": file_obj.id,
# #                 "status": "FAILED",
# #                 "error": error_msg
# #             })

# #         raise self.retry(exc=e, countdown=10)


# from celery import shared_task
# import os
# import zipfile
# import traceback

# from ingestion.models import DSXFile
# from .parser import DSXParser
# from agent.agent import DSXAgent
# from processing.sttm_excel import generate_sttm_excel

# from channels.layers import get_channel_layer
# from asgiref.sync import async_to_sync


# # =========================================================
# # WEBSOCKET
# # =========================================================
# def send_ws_update(batch_id, data):
#     channel_layer = get_channel_layer()

#     async_to_sync(channel_layer.group_send)(
#         f"batch_{batch_id}",
#         {
#             "type": "send_update",
#             "data": data
#         }
#     )


# # =========================================================
# # SAVE DBT FILES
# # =========================================================
# def save_dbt_files(file_obj, dbt_files):
#     base = f"media/dbt/{file_obj.batch.id}/{file_obj.id}/"

#     for path, content in dbt_files.items():
#         full_path = os.path.join(base, path)
#         os.makedirs(os.path.dirname(full_path), exist_ok=True)

#         with open(full_path, "w", encoding="utf-8") as f:
#             f.write(content)


# def zip_dbt_project(file_obj):
#     folder = f"media/dbt/{file_obj.batch.id}/{file_obj.id}"
#     zip_path = f"media/dbt/{file_obj.batch.id}/{file_obj.id}.zip"

#     with zipfile.ZipFile(zip_path, 'w') as z:
#         for root, _, files in os.walk(folder):
#             for file in files:
#                 full = os.path.join(root, file)
#                 z.write(full, arcname=os.path.relpath(full, folder))

#     return zip_path


# # =========================================================
# # PROCESS BATCH
# # =========================================================
# @shared_task
# def process_batch(batch_id):
#     from processing.tasks import process_file

#     files = DSXFile.objects.filter(batch_id=batch_id)

#     for f in files:
#         process_file.delay(f.id)


# # =========================================================
# # PROCESS FILE
# # =========================================================
# @shared_task(bind=True, max_retries=2)
# def process_file(self, file_id):

#     file_obj = None

#     try:
#         file_obj = DSXFile.objects.get(id=file_id)

#         # -------------------------------
#         # START
#         # -------------------------------
#         file_obj.status = "PROCESSING"
#         file_obj.save()

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "status": "PROCESSING",
#             "step": "START"
#         })

#         # -------------------------------
#         # PARSING
#         # -------------------------------
#         parser = DSXParser()
#         parsed = parser.parse(file_obj.file.path)

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "PARSING"
#         })

#         print("\n===== PARSED =====\n", parsed)

#         # -------------------------------
#         # AI PROCESSING
#         # -------------------------------
#         agent = DSXAgent()
#         result = agent.run(parsed)

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "AI_PROCESSING"
#         })

#         # -------------------------------
#         # STTM
#         # -------------------------------
#         sttm = result.get("sttm", [])

#         excel_path = generate_sttm_excel(file_obj, sttm)

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "STTM_GENERATED",
#             "sttm": sttm
#         })

#         # -------------------------------
#         # SQL (DBT SQL)
#         # -------------------------------
#         sql = result.get("snowflake_sql", "")

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "SQL_GENERATED",
#             "snowflake_sql": sql
#         })

#         # -------------------------------
#         # DBT FILES
#         # -------------------------------
#         dbt_files = result.get("dbt_files", {})

#         dbt_zip = None

#         if dbt_files:
#             save_dbt_files(file_obj, dbt_files)
#             dbt_zip = zip_dbt_project(file_obj)

#         # -------------------------------
#         # DOCUMENTATION
#         # -------------------------------
#         documentation = result.get("documentation", "")

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "DOC_GENERATED",
#             "documentation": documentation
#         })

#         # -------------------------------
#         # SAVE DB
#         # -------------------------------
#         file_obj.sttm_json = sttm
#         file_obj.snowflake_sql = sql
#         file_obj.documentation = documentation
#         file_obj.sttm_excel = excel_path

#         if dbt_zip:
#             file_obj.dbt_zip = dbt_zip

#         file_obj.status = "DONE"
#         file_obj.save()

#         # -------------------------------
#         # FINAL UPDATE
#         # -------------------------------
#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "status": "DONE",
#             "step": "DONE",
#             "sttm": sttm,
#             "snowflake_sql": sql,
#             "documentation": documentation,
#             "dbt_zip": dbt_zip
#         })

#     except Exception as e:

#         error_msg = str(e)
#         print("\n===== ERROR =====\n", traceback.format_exc())

#         if file_obj:
#             file_obj.status = "FAILED"
#             file_obj.save()

#             send_ws_update(file_obj.batch.id, {
#                 "file_id": file_obj.id,
#                 "status": "FAILED",
#                 "error": error_msg
#             })

#         raise self.retry(exc=e, countdown=10)


# from celery import shared_task
# import os
# import zipfile
# import traceback
# import json

# from ingestion.models import DSXFile
# from .parser import DSXParser
# from agent.agent import DSXAgent
# from processing.sttm_excel import generate_sttm_excel

# from channels.layers import get_channel_layer
# from asgiref.sync import async_to_sync


# # =========================================================
# # WEBSOCKET
# # =========================================================
# def send_ws_update(batch_id, data):
#     channel_layer = get_channel_layer()

#     async_to_sync(channel_layer.group_send)(
#         f"batch_{batch_id}",
#         {
#             "type": "send_update",
#             "data": data
#         }
#     )


# # =========================================================
# # SAVE DBT FILES
# # =========================================================
# def save_dbt_files(file_obj, dbt_files):

#     base = f"media/dbt/{file_obj.batch.id}/{file_obj.id}/"

#     # ------------------------------
#     # SAFETY CHECK
#     # ------------------------------
#     if not isinstance(dbt_files, dict):
#         print("❌ DBT FILES INVALID:", type(dbt_files))
#         return

#     for path, content in dbt_files.items():

#         try:
#             full_path = os.path.join(base, path)
#             os.makedirs(os.path.dirname(full_path), exist_ok=True)

#             # ------------------------------
#             # 🔥 CRITICAL FIX
#             # ------------------------------
#             if isinstance(content, dict):
#                 content = json.dumps(content, indent=2)

#             elif isinstance(content, list):
#                 content = json.dumps(content, indent=2)

#             elif content is None:
#                 content = ""

#             elif not isinstance(content, str):
#                 content = str(content)

#             # ------------------------------
#             # WRITE FILE
#             # ------------------------------
#             with open(full_path, "w", encoding="utf-8") as f:
#                 f.write(content)

#         except Exception as e:
#             print(f"❌ FAILED TO WRITE FILE: {path}")
#             print("ERROR:", str(e))


# # =========================================================
# # PROCESS BATCH
# # =========================================================
# @shared_task
# def process_batch(batch_id):
#     from processing.tasks import process_file

#     files = DSXFile.objects.filter(batch_id=batch_id)

#     for f in files:
#         process_file.delay(f.id)


# # =========================================================
# # PROCESS FILE
# # =========================================================
# @shared_task(bind=True, max_retries=2)
# def process_file(self, file_id):

#     file_obj = None

#     try:
#         file_obj = DSXFile.objects.get(id=file_id)

#         # -------------------------------
#         # START
#         # -------------------------------
#         file_obj.status = "PROCESSING"
#         file_obj.save()

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "status": "PROCESSING",
#             "step": "START"
#         })

#         # -------------------------------
#         # PARSING
#         # -------------------------------
#         parser = DSXParser()
#         parsed = parser.parse(file_obj.file.path)

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "PARSING"
#         })

#         print("\n===== PARSED =====\n", parsed)

#         # -------------------------------
#         # AI PROCESSING
#         # -------------------------------
#         agent = DSXAgent()
#         result = agent.run(parsed)

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "AI_PROCESSING"
#         })

#         # -------------------------------
#         # STTM
#         # -------------------------------
#         sttm = result.get("sttm", [])

#         excel_path = generate_sttm_excel(file_obj, sttm)

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "STTM_GENERATED",
#             "sttm": sttm
#         })

#         # -------------------------------
#         # SQL (DBT SQL ONLY)
#         # -------------------------------
#         sql = result.get("snowflake_sql", "")

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "SQL_GENERATED",
#             "snowflake_sql": sql
#         })

#         # -------------------------------
#         # DBT FILES (NO ZIP)
#         # -------------------------------
#         dbt_files = result.get("dbt_files", {})

#         if isinstance(dbt_files, dict) and dbt_files:
#             base = f"media/dbt/{file_obj.batch.id}/{file_obj.id}/"

#             for path, content in dbt_files.items():
#                 full_path = os.path.join(base, path)
#                 os.makedirs(os.path.dirname(full_path), exist_ok=True)

#                 # 🔥 FIX: ensure string write
#                 if isinstance(content, dict):
#                     import json
#                     content = json.dumps(content, indent=2)

#                 with open(full_path, "w", encoding="utf-8") as f:
#                     f.write(str(content))

#         # -------------------------------
#         # DOCUMENTATION
#         # -------------------------------
#         documentation = result.get("documentation", "")

#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "step": "DOC_GENERATED",
#             "documentation": documentation
#         })

#         # -------------------------------
#         # SAVE DB
#         # -------------------------------
#         file_obj.sttm_json = sttm
#         file_obj.snowflake_sql = sql
#         file_obj.documentation = documentation
#         file_obj.sttm_excel = excel_path
#         file_obj.status = "DONE"
#         file_obj.save()

#         # -------------------------------
#         # FINAL UPDATE
#         # -------------------------------
#         send_ws_update(file_obj.batch.id, {
#             "file_id": file_obj.id,
#             "status": "DONE",
#             "step": "DONE",
#             "sttm": sttm,
#             "snowflake_sql": sql,
#             "documentation": documentation
#         })

#     except Exception as e:

#         error_msg = str(e)
#         print("\n===== ERROR =====\n", traceback.format_exc())

#         if file_obj:
#             file_obj.status = "FAILED"
#             file_obj.save()

#             send_ws_update(file_obj.batch.id, {
#                 "file_id": file_obj.id,
#                 "status": "FAILED",
#                 "error": error_msg
#             })

#         raise self.retry(exc=e, countdown=10)


from celery import shared_task
import os
import traceback
import json

from ingestion.models import DSXFile
from .parser import DSXParser
from agent.agent import DSXAgent
from processing.sttm_excel import generate_sttm_excel

from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync


# =========================================================
# WEBSOCKET
# =========================================================
def send_ws_update(batch_id, data):
    channel_layer = get_channel_layer()

    async_to_sync(channel_layer.group_send)(
        f"batch_{batch_id}",
        {
            "type": "send_update",
            "data": data
        }
    )


# =========================================================
# SAVE DBT FILES (FIXED STRUCTURE SUPPORT)
# =========================================================
def save_dbt_files(file_obj, dbt_files):

    base = f"media/dbt/{file_obj.batch.id}/{file_obj.id}/"

    if not isinstance(dbt_files, dict):
        print("❌ DBT FILES INVALID:", type(dbt_files))
        return

    for path, content in dbt_files.items():
        try:
            full_path = os.path.join(base, path)

            # ensure folders exist
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            # 🔥 ensure string content
            if isinstance(content, (dict, list)):
                content = json.dumps(content, indent=2)
            elif content is None:
                content = ""
            elif not isinstance(content, str):
                content = str(content)

            with open(full_path, "w", encoding="utf-8") as f:
                f.write(content)

        except Exception as e:
            print(f"❌ FAILED TO WRITE FILE: {path}")
            print("ERROR:", str(e))


# =========================================================
# PROCESS BATCH
# =========================================================
@shared_task
def process_batch(batch_id):
    from processing.tasks import process_file

    files = DSXFile.objects.filter(batch_id=batch_id)

    for f in files:
        process_file.delay(f.id)


# =========================================================
# PROCESS FILE (FULL FIXED PIPELINE)
# =========================================================
@shared_task(bind=True, max_retries=2)
def process_file(self, file_id):

    file_obj = None

    try:
        file_obj = DSXFile.objects.get(id=file_id)

        # ==================================================
        # START
        # ==================================================
        file_obj.status = "PROCESSING"
        file_obj.save()

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "file_name": file_obj.file.name,  # ✅ FIX undefined
            "status": "PROCESSING",
            "step": "START"
        })

        # ==================================================
        # PARSING
        # ==================================================
        parser = DSXParser()
        parsed = parser.parse(file_obj.file.path)

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "PARSING"
        })

        # ==================================================
        # AI PROCESSING
        # ==================================================
        agent = DSXAgent()
        result = agent.run(parsed)

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "AI_PROCESSING"
        })

        # ==================================================
        # STTM
        # ==================================================
        sttm = result.get("sttm", []) or []

        excel_path = generate_sttm_excel(file_obj, sttm)

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "STTM_GENERATED",
            "sttm": sttm
        })

        # ==================================================
        # DBT FILES (WRITE TO DISK)
        # ==================================================
        dbt_files = result.get("dbt_files", {}) or {}

        save_dbt_files(file_obj, dbt_files)

        # ==================================================
        # SQL (CRITICAL FIX)
        # ==================================================
        snowflake_sql = result.get("snowflake_sql", "")

        if not snowflake_sql or snowflake_sql.strip() == "":
            snowflake_sql = "-- No SQL generated"

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "SQL_GENERATED",
            "snowflake_sql": snowflake_sql
        })

        # ==================================================
        # DOCUMENTATION
        # ==================================================
        documentation = result.get("documentation", "") or ""

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "DOC_GENERATED",
            "documentation": documentation
        })

        # ==================================================
        # SAVE TO DB
        # ==================================================
        file_obj.sttm_json = sttm
        file_obj.snowflake_sql = snowflake_sql
        file_obj.documentation = documentation
        file_obj.sttm_excel = excel_path
        file_obj.status = "DONE"
        file_obj.save()

        # ==================================================
        # FINAL UPDATE (FULL PAYLOAD)
        # ==================================================
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "file_name": file_obj.file.name,  # ✅ FIX
            "status": "DONE",
            "step": "DONE",
            "sttm": sttm,
            "snowflake_sql": snowflake_sql,
            "documentation": documentation,
            "dbt_files": dbt_files  # ✅ UI TREE FIX
        })

    except Exception as e:

        error_msg = str(e)

        print("\n===== ERROR =====\n", traceback.format_exc())

        if file_obj:
            file_obj.status = "FAILED"
            file_obj.save()

            send_ws_update(file_obj.batch.id, {
                "file_id": file_obj.id,
                "file_name": file_obj.file.name if file_obj else "",
                "status": "FAILED",
                "error": error_msg
            })

        raise self.retry(exc=e, countdown=10)