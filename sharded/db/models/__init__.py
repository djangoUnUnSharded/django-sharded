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
from django.conf import settings
from django.db import connections
from django.core.exceptions import ObjectDoesNotExist
from sharded.db.models.manager import ShardedManager
from sharded.db.models.query import ShardedQuerySet, ShardedValuesQuerySet, ShardedValuesListQuerySet
from django.db import transaction
#from sharded.db.db_models import BucketCounter
from time import time
from random import randint
from django.db import models
from django.db.models import *


NUM_BUCKETS = getattr(settings, 'NUM_BUCKETS', 2048)
print("sharded.db.models.init: imports complete, NUM_BUCKETS=", NUM_BUCKETS)
bucket_counts = {}

class BucketCounter(models.Model):
    id = models.IntegerField(primary_key=True)
    counter = models.IntegerField(default=0)

#    objects = models.Manager()
    def save(self, *args, **kwargs):
        print("sharded.db.models.init: BucketCounter #save: self, args, kwargs = ", self, args, kwargs)
        super(BucketCounter, self).save(*args, **kwargs)

def get_counter(bucket_id):
    print("sharded.db.models.init: get_counter, bucketid: ", bucket_id)
#    try:
#        buck = BucketCounter.objects.get(id=bucket_id)
#    except ObjectDoesNotExist:
#    buck = BucketCounter(id=bucket_id, counter=0)
#
#    buck.counter = F('counter') + 1
#
#    if buck.counter > 1023:
#        buck.counter = F('counter') % 1024

#    buck.save()
    bucket_count = getattr(bucket_counts, 'bucket_id', 0)
    bucket_counts['bucket_id'] = bucket_count + 1
    print("sharded.db.models.init: get_counter, bucket.counter: ", bucket_count)
    return bucket_count



def gen_id():
    time_stp = int((round(time() * 1000)))
    print("sharded.db.models.init.gen_id, time=", time_stp)
    u_id = time_stp << (64 - 41)
    print("sharded.db.models.init.gen_id: id=", u_id)
    bucket_id = randint(0, NUM_BUCKETS - 1)
    print("sharded.db.models.init.gen_id: bucket_id=", bucket_id)
    u_id |= bucket_id << (64 - 41 - 13)
    print("sharded.db.models.init.gen_id: id=", u_id)
    counter = get_counter(bucket_id)
    print("sharded.db.models.init.gen_id: counter=", counter)
    u_id |= counter
    print("sharded.db.models.init.gen_id: id=", u_id)
    print("sharded.db.models.init: gen_id: ", bucket_id,
          get_counter(bucket_id), u_id)

    return {'u_id': u_id, 'bucket_id': bucket_id}

class ShardedModelMixin(object):
    def __int__(self):  # define integer casting action
        print("sharded.db.models.init: ShardedModelMixin to_int")
        print(self)
        return self.id

    def cursor(self):
        print("sharded.db.models.init: ShardedModelMixin: getting cursor")
        return connections[self._state.db].cursor()



class Sharded64Model(ShardedModelMixin, Model):
    print("sharded.db.models.init: Sharded64Model initializing")
    id = BigIntegerField(primary_key=True, editable=False, default=gen_id()['u_id'])
    bucket_id = BigIntegerField(editable=False, default=gen_id()['bucket_id'])

    objects = ShardedManager()

    class Meta:
        abstract = True




ShardedModel = Sharded64Model
