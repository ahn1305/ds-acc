from celery import shared_task
import traceback

from ingestion.models import InformaticaFile
from agent.agent import InformaticaPipeline

from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync


# =========================================
# WEBSOCKET
# =========================================
def send_ws_update(batch_id, data):
    channel_layer = get_channel_layer()

    async_to_sync(channel_layer.group_send)(
        f"batch_{batch_id}",
        {
            "type": "send_update",
            "data": data
        }
    )


# =========================================
# PROCESS BATCH
# =========================================
@shared_task
def process_informatica_batch(batch_id):

    files = InformaticaFile.objects.filter(batch_id=batch_id)

    for f in files:
        process_informatica_file.delay(f.id)


# =========================================
# PROCESS FILE
# =========================================
@shared_task(bind=True, max_retries=2)
def process_informatica_file(self, file_id):

    file_obj = None

    try:
        file_obj = InformaticaFile.objects.get(id=file_id)

        # -------------------------------
        # START
        # -------------------------------
        file_obj.status = "PROCESSING"
        file_obj.save()

        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "file_name": file_obj.file.name,
            "status": "PROCESSING",
            "step": "START"
        })

        # -------------------------------
        # RUN PIPELINE
        # -------------------------------
        pipeline = InformaticaPipeline()

        # 🔥 STEP: PARSING
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "PARSING"
        })

        result = pipeline.run(file_obj.file.path)

        # 🔥 STEP: GRAPH
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "GRAPH"
        })

        # 🔥 STEP: LINEAGE
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "LINEAGE"
        })

        # 🔥 STEP: STTM
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "STTM"
        })

        # 🔥 STEP: SQL
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "step": "SQL"
        })

        # -------------------------------
        # EXTRACT RESULTS
        # -------------------------------
        parsed = result.get("parsed", {})
        graph = result.get("graph", {})
        lineage = result.get("lineage", [])
        sttm = result.get("sttm", [])
        sql_models = result.get("sql_models", {})
        sql = result.get("snowflake_sql", "")
        documentation = result.get("documentation", "")

        # -------------------------------
        # SAVE TO DB
        # -------------------------------
        file_obj.sttm_json = sttm
        file_obj.snowflake_sql = sql
        file_obj.documentation = documentation
        file_obj.status = "DONE"
        file_obj.save()

        # -------------------------------
        # FINAL UPDATE (🔥 IMPORTANT FIX)
        # -------------------------------
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "file_name": file_obj.file.name,
            "status": "DONE",
            "step": "DONE",

            # 🔥 FULL DATA (THIS FIXES YOUR UI)
            "parsed": parsed,
            "graph": graph,
            "lineage": lineage,
            "sttm": sttm,
            "sql_models": sql_models,
            "snowflake_sql": sql,
            "documentation": documentation
        })

    except Exception as e:

        error_msg = str(e)
        print("\n===== INFORMATICA ERROR =====\n", traceback.format_exc())

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