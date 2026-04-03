# Generated migration for BatchJob completed_at field

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ingestion", "0005_informaticafile_extended_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="batchjob",
            name="completed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
