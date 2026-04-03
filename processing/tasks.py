from celery import shared_task
import os
import traceback
import json

from ingestion.models import DSXFile, BatchJob
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
# CHECK & MARK BATCH COMPLETION
# =========================================================
def check_and_mark_batch_complete(batch_id):
    """Check if all files in batch are done, and mark batch as completed"""
    from django.utils import timezone
    
    batch = DSXFile.objects.filter(batch_id=batch_id)
    all_done = all(f.status in ["DONE", "FAILED"] for f in batch)
    
    if all_done:
        batch_obj = BatchJob.objects.get(id=batch_id)
        batch_obj.completed_at = timezone.now()
        batch_obj.save()
        
        # Send final batch update
        send_ws_update(batch_id, {
            "type": "BATCH_COMPLETE",
            "batch_status": "COMPLETE",
            "timestamp": timezone.now().isoformat()
        })


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
        # DATA MODEL 
        # ==================================================

        data_model = result.get("data_model","") or ""
        send_ws_update(file_obj.batch.id, {
            "file_id":file_obj.id,
            "step":"DATA_MODEL GENERATED",
            "data_model":data_model
        })

        # ==================================================
        # ER DIAGRAM 
        # ==================================================

        er_diagram = result.get("er_diagram","") or ""
        send_ws_update(file_obj.batch.id, {
            "file_id":file_obj.id,
            "step":"ER_DIAGRAM GENERATED",
            "er_diagram":er_diagram
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
        from django.utils import timezone
        
        file_obj.sttm_json = sttm
        file_obj.snowflake_sql = snowflake_sql
        file_obj.documentation = documentation
        file_obj.dbt_sql = result.get("dbt_sql", "")
        file_obj.dbt_files = dbt_files
        file_obj.data_model = data_model
        file_obj.er_diagram = er_diagram
        file_obj.sttm_excel = excel_path
        file_obj.status = "DONE"
        file_obj.completed_at = timezone.now()
        file_obj.save()

        # ==================================================
        # CALCULATE PROCESSING TIME
        # ==================================================
        file_start = file_obj.created_at or file_obj.batch.created_at
        file_end = file_obj.completed_at
        processing_time = (file_end - file_start).total_seconds() if file_start else 0

        # ==================================================
        # FINAL UPDATE (FULL PAYLOAD)
        # ==================================================
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "file_name": file_obj.file.name,  # ✅ FIX
            "status": "DONE",
            "step": "DONE",
            "processing_time_seconds": round(processing_time, 2),
            "sttm": sttm,
            "snowflake_sql": snowflake_sql,
            "data_model":data_model,
            "documentation": documentation,
            "dbt_files": dbt_files  # ✅ UI TREE FIX
        })
        
        # ==================================================
        # CHECK IF BATCH IS COMPLETE
        # ==================================================
        check_and_mark_batch_complete(file_obj.batch.id)

    except Exception as e:

        error_msg = str(e)

        print("\n===== ERROR =====\n", traceback.format_exc())

        if file_obj:
            from django.utils import timezone
            
            file_obj.status = "FAILED"
            file_obj.completed_at = timezone.now()
            file_obj.save()

            send_ws_update(file_obj.batch.id, {
                "file_id": file_obj.id,
                "file_name": file_obj.file.name if file_obj else "",
                "status": "FAILED",
                "error": error_msg
            })
            
            # Check if batch should be marked complete
            check_and_mark_batch_complete(file_obj.batch.id)

        raise self.retry(exc=e, countdown=10)