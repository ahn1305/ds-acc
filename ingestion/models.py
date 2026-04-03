from django.db import models

# Create your models here.
# ingestion/models.py

class BatchJob(models.Model):
    status = models.CharField(max_length=50, default="PENDING")
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)



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
    sttm_excel = models.FileField(upload_to="sttm/", null=True, blank=True)

    snowflake_sql = models.TextField(null=True, blank=True)
    dbt_sql = models.TextField(null=True, blank=True)
    dbt_files = models.JSONField(null=True, blank=True)
    
    data_model = models.TextField(null=True, blank=True)
    er_diagram = models.TextField(null=True, blank=True)
    
    documentation = models.TextField(null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)



class InformaticaFile(models.Model):
    batch = models.ForeignKey("BatchJob", on_delete=models.CASCADE)
    file = models.FileField(upload_to="informatica/")

    status = models.CharField(max_length=50, default="PENDING")

    sttm_json = models.JSONField(null=True, blank=True)
    snowflake_sql = models.TextField(null=True, blank=True)
    documentation = models.TextField(null=True, blank=True)
    
    data_model = models.TextField(null=True, blank=True)
    er_diagram = models.TextField(null=True, blank=True)
    parsed_graph = models.JSONField(null=True, blank=True)
    lineage_data = models.JSONField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)