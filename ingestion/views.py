# ingestion/views.py

from rest_framework.views import APIView
from rest_framework.response import Response
from .models import BatchJob, DSXFile, InformaticaFile
from processing.informatica_tasks import process_informatica_batch
from django.utils import timezone

class UploadView(APIView):

    def post(self, request):
        files = request.FILES.getlist("files")

        batch = BatchJob.objects.create()
        batch.created_at = timezone.now()
        batch.save()

        for f in files:
            DSXFile.objects.create(batch=batch, file=f)

        from processing.tasks import process_batch
        process_batch.delay(batch.id)

        return Response({
            "batch_id": batch.id,
            "file_count": len(files),
            "created_at": batch.created_at.isoformat()
        })

# ingestion/views.py

# ingestion/views.py

class BatchDetailView(APIView):
    def get(self, request, batch_id):
        from django.utils import timezone
        
        batch = BatchJob.objects.get(id=batch_id)
        files = DSXFile.objects.filter(batch_id=batch_id)

        data = []
        
        # Calculate batch timing
        batch_start = batch.created_at
        batch_end = batch.completed_at or timezone.now()
        total_batch_time = (batch_end - batch_start).total_seconds()

        for f in files:
            file_start = f.created_at or batch_start
            file_end = f.completed_at or batch_end
            file_processing_time = (file_end - file_start).total_seconds() if f.status == "DONE" else 0
            
            data.append({
                "id": f.id,
                "name": f.file.name,
                "status": f.status,
                "created_at": f.created_at.isoformat() if f.created_at else None,
                "completed_at": f.completed_at.isoformat() if f.completed_at else None,
                "processing_time_seconds": round(file_processing_time, 2),
                "sttm": f.sttm_json,
                "sttm_file": f.sttm_file.url if f.sttm_file else None,
                "sttm_excel": f.sttm_excel.url if f.sttm_excel else None,
                "snowflake_sql": f.snowflake_sql,
                "dbt_sql": f.dbt_sql,
                "dbt_files": f.dbt_files,
                "data_model": f.data_model,
                "er_diagram": f.er_diagram,
                "documentation": f.documentation
            })

        return Response({
            "batch_id": batch_id,
            "batch_status": batch.status,
            "batch_created_at": batch.created_at.isoformat(),
            "batch_completed_at": batch.completed_at.isoformat() if batch.completed_at else None,
            "batch_total_time_seconds": round(total_batch_time, 2),
            "file_count": files.count(),
            "files": data
        })




class InformaticaUploadView(APIView):

    def post(self, request):

        files = request.FILES.getlist("files")

        batch = BatchJob.objects.create()
        batch.created_at = timezone.now()
        batch.save()

        for f in files:
            InformaticaFile.objects.create(batch=batch, file=f)

        # 🔥 trigger celery
        process_informatica_batch.delay(batch.id)

        return Response({
            "batch_id": batch.id,
            "file_count": len(files),
            "created_at": batch.created_at.isoformat()
        })
    

class InformaticaBatchDetailView(APIView):

    def get(self, request, batch_id):
        from django.utils import timezone
        
        batch = BatchJob.objects.get(id=batch_id)
        files = InformaticaFile.objects.filter(batch_id=batch_id)

        data = []
        
        # Calculate batch timing
        batch_start = batch.created_at
        batch_end = batch.completed_at or timezone.now()
        total_batch_time = (batch_end - batch_start).total_seconds()

        for f in files:
            file_start = f.created_at or batch_start
            file_end = f.completed_at or batch_end
            file_processing_time = (file_end - file_start).total_seconds() if f.status == "DONE" else 0
            
            data.append({
                "id": f.id,
                "name": f.file.name,
                "status": f.status,
                "created_at": f.created_at.isoformat() if f.created_at else None,
                "completed_at": f.completed_at.isoformat() if f.completed_at else None,
                "processing_time_seconds": round(file_processing_time, 2),
                "sttm": f.sttm_json,
                "snowflake_sql": f.snowflake_sql,
                "data_model": f.data_model,
                "er_diagram": f.er_diagram,
                "parsed_graph": f.parsed_graph,
                "lineage_data": f.lineage_data,
                "documentation": f.documentation
            })

        return Response({
            "batch_id": batch_id,
            "batch_status": batch.status,
            "batch_created_at": batch.created_at.isoformat(),
            "batch_completed_at": batch.completed_at.isoformat() if batch.completed_at else None,
            "batch_total_time_seconds": round(total_batch_time, 2),
            "file_count": files.count(),
            "files": data
        })