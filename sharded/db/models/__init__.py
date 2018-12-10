"""
Copyright 2016 Vimal Aravindashan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from django.db.models import *
from django.db import connections
from sharded.db.routers import NUM_BUCKETS
from django.core.exceptions import ObjectDoesNotExist
from sharded.db.models.manager import ShardedManager
from sharded.db.models.query import ShardedQuerySet, ShardedValuesQuerySet, ShardedValuesListQuerySet
from django.db import transaction
from sharded.models import BucketCounter
from time import time
from random import randint


def gen_id():
    id = int((round(time() * 1000))) << (64 - 41)
    bucket_id = randint(0, NUM_BUCKETS - 1)
    id |= bucket_id << (64 - 41 - 13)
    id |= get_counter(bucket_id)

    return {id: id, bucket_id: bucket_id}


class ShardedModelMixin(object):
    def __int__(self):  # define integer casting action
        return self.id

    def cursor(self):
        return connections[self._state.db].cursor()




class Sharded64Model(ShardedModelMixin, Model):
    id = BigIntegerField(primary_key=True, editable=False, default=gen_id()['id'])
    bucket_id = BigIntegerField(editable=False, default=gen_id()['bucket_id'])
    
    objects = ShardedManager()

    class Meta:
        abstract = True



@transaction.atomic
def get_counter(bucket_id):
    try:
        buck = BucketCounter.objects.get(id=bucket_id)
    except ObjectDoesNotExist:
        buck = BucketCounter(id=bucket_id, counter=0)

    buck.counter = F('counter') + 1

    if buck.counter > 1023:
        buck.counter = F('counter') % 1024

    buck.save()
    return buck.counter


ShardedModel = Sharded64Model
