# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='MigrationCommandModel',
            fields=[
                ('id', models.DateTimeField(auto_now=True)),
                ('command', models.TextField(serialize=False, primary_key=True)),
                ('complete', models.BooleanField(default=False)),
            ],
        ),
    ]
