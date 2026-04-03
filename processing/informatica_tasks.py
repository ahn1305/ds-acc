from celery import shared_task
import traceback

from ingestion.models import InformaticaFile, BatchJob
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
# CHECK & MARK BATCH COMPLETION
# =========================================
def check_and_mark_batch_complete(batch_id):
    """Check if all files in batch are done, and mark batch as completed"""
    from django.utils import timezone
    
    batch_files = InformaticaFile.objects.filter(batch_id=batch_id)
    all_done = all(f.status in ["DONE", "FAILED"] for f in batch_files)
    
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
        data_model = result.get("data_model", "")
        er_diagram = result.get("er_diagram", "")

        # -------------------------------
        # SAVE TO DB
        # -------------------------------
        from django.utils import timezone
        
        file_obj.sttm_json = sttm
        file_obj.snowflake_sql = sql
        file_obj.documentation = documentation
        file_obj.data_model = data_model
        file_obj.er_diagram = er_diagram
        file_obj.parsed_graph = graph
        file_obj.lineage_data = lineage
        file_obj.status = "DONE"
        file_obj.completed_at = timezone.now()
        file_obj.save()

        # Calculate processing time
        from django.utils import timezone
        file_start = file_obj.created_at or file_obj.batch.created_at
        file_end = file_obj.completed_at
        processing_time = (file_end - file_start).total_seconds() if file_start else 0

        # FINAL UPDATE (🔥 IMPORTANT FIX)
        send_ws_update(file_obj.batch.id, {
            "file_id": file_obj.id,
            "file_name": file_obj.file.name,
            "status": "DONE",
            "step": "DONE",
            "processing_time_seconds": round(processing_time, 2),

            # 🔥 FULL DATA (THIS FIXES YOUR UI)
            "parsed": parsed,
            "graph": graph,
            "lineage": lineage,
            "sttm": sttm,
            "sql_models": sql_models,
            "snowflake_sql": sql,
            "data_model": data_model,
            "er_diagram": er_diagram,
            "documentation": documentation
        })
        
        # Check if batch is complete
        check_and_mark_batch_complete(file_obj.batch.id)

    except Exception as e:

        error_msg = str(e)
        print("\n===== INFORMATICA ERROR =====\n", traceback.format_exc())

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