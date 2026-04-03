# Generated migration for extended DSXFile fields

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ingestion", "0003_informaticafile"),
    ]

    operations = [
        migrations.AddField(
            model_name="dsxfile",
            name="sttm_excel",
            field=models.FileField(blank=True, null=True, upload_to="sttm/"),
        ),
        migrations.AddField(
            model_name="dsxfile",
            name="dbt_files",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="dsxfile",
            name="data_model",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="dsxfile",
            name="er_diagram",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="dsxfile",
            name="created_at",
            field=models.DateTimeField(auto_now_add=True, null=True),
        ),
        migrations.AddField(
            model_name="dsxfile",
            name="completed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
