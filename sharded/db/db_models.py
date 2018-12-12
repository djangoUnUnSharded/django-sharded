from django.db import models
from django.db.models import *

class BucketCounter(models.Model):
    id = models.IntegerField(primary_key=True)
    counter = models.IntegerField(default=0)

#    objects = models.Manager()
