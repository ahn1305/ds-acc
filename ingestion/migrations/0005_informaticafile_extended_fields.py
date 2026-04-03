# Generated migration for extended InformaticaFile fields

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ingestion", "0004_dsxfile_extended_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="informaticafile",
            name="data_model",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="informaticafile",
            name="er_diagram",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="informaticafile",
            name="parsed_graph",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="informaticafile",
            name="lineage_data",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="informaticafile",
            name="completed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
