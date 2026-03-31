from django.db import models

# Create your models here.
# ingestion/models.py

class BatchJob(models.Model):
    status = models.CharField(max_length=50, default="PENDING")
    created_at = models.DateTimeField(auto_now_add=True)



class OutputFile(models.Model):
    batch = models.ForeignKey(BatchJob, on_delete=models.CASCADE)
    file = models.FileField(upload_to="output/")

# ingestion/models.py

class DSXFile(models.Model):
    batch = models.ForeignKey("BatchJob", on_delete=models.CASCADE)
    file = models.FileField(upload_to="dsx/")
    status = models.CharField(max_length=50, default="UPLOADED")

    sttm_json = models.JSONField(null=True, blank=True)
    sttm_file = models.FileField(upload_to="sttm/", null=True, blank=True)

    snowflake_sql = models.TextField(null=True, blank=True)
    dbt_sql = models.TextField(null=True, blank=True)
    documentation = models.TextField(null=True, blank=True)
