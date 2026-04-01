# ingestion/views.py

from rest_framework.views import APIView
from rest_framework.response import Response
from .models import BatchJob, DSXFile, InformaticaFile
from processing.informatica_tasks import process_informatica_batch

class UploadView(APIView):

    def post(self, request):
        files = request.FILES.getlist("files")

        batch = BatchJob.objects.create()

        for f in files:
            DSXFile.objects.create(batch=batch, file=f)

        from processing.tasks import process_batch
        process_batch.delay(batch.id)

        return Response({"batch_id": batch.id})

# ingestion/views.py

class BatchDetailView(APIView):
    def get(self, request, batch_id):
        files = DSXFile.objects.filter(batch_id=batch_id)

        data = []

        for f in files:
            data.append({
                "id": f.id,
                "name": f.file.name,
                "status": f.status,
                "sttm": f.sttm_json,
                "sttm_file": f.sttm_file.url if f.sttm_file else None,
                "snowflake_sql": f.snowflake_sql,
                "dbt_sql": f.dbt_sql,
                "documentation": f.documentation
            })

        return Response({"files": data})




class InformaticaUploadView(APIView):

    def post(self, request):

        files = request.FILES.getlist("files")

        batch = BatchJob.objects.create()

        for f in files:
            InformaticaFile.objects.create(batch=batch, file=f)

        # 🔥 trigger celery
        process_informatica_batch.delay(batch.id)

        return Response({"batch_id": batch.id})
    

class InformaticaBatchDetailView(APIView):

    def get(self, request, batch_id):

        files = InformaticaFile.objects.filter(batch_id=batch_id)

        data = []

        for f in files:
            data.append({
                "id": f.id,
                "name": f.file.name,
                "status": f.status,
                "sttm": f.sttm_json,
                "snowflake_sql": f.snowflake_sql,
                "documentation": f.documentation
            })

        return Response({"files": data})