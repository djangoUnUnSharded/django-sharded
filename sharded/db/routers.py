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

from django.apps import apps
from django.conf import settings
from django.utils import six
from django.db.models.fields.related import RelatedField, ForeignObjectRel
from sharded.db import models, SHARDED_DB_PREFIX, shards
from sharded.db.models import Sharded64Model, ShardedModelMixin
from sharded.exceptions import ShardCouldNotBeDetermined

print("beginning routers.py")
NUM_BUCKETS = getattr(settings, 'NUM_BUCKETS', 2048) + 1
BUCKET_DICT = {}
SHARD_DICT = {}


def create_bucket_dict():
    for bucket in range(1, NUM_BUCKETS):
        shard = bucket % len(shards) + 1
        BUCKET_DICT[bucket] = (shard, -1)
        if shard in SHARD_DICT:
            SHARD_DICT[shard].append(bucket)
        else:
            SHARD_DICT[shard] = [bucket]


def bucket_to_shard(bucket_id):
    if bucket_id > NUM_BUCKETS or bucket_id < 1:
        raise Exception("Bucket out of bounds %d" % bucket_id)
    (num_old, num_new) = BUCKET_DICT[bucket_id + 1]
    db = num_old
    new_db = num_new
    return db, new_db


def id_to_bucket_id(u_id):
    bucket_id = int(u_id)
    bucket_id = bucket_id >> 10
    bucket_id = bucket_id & 0x1FFF

    return bucket_id


class ShardedRouter(object):
    def __init__(self):
        print("sharded router init", self)
        self.sharded_tables = set()  # set of sharded tables
        len_sharded_tables = -1
        while len_sharded_tables != len(self.sharded_tables):
            len_sharded_tables = len(self.sharded_tables)
            # for every model that is NOT in the set of sharded tables
            for model in filter(lambda m: m._meta.db_table not in self.sharded_tables,
                                apps.get_models(include_auto_created=True)):
                for field in model._meta.get_fields(include_hidden=True):
                    # for every field that is in those models
                    # for field in model._meta.get_fields(include_hidden=True):
                    # if those fields are an instance of the sharded fields
                    if Sharded64Model in model.mro() or \
                            (isinstance(field, (RelatedField, ForeignObjectRel)) and
                             (isinstance(field.related_model,
                                         six.string_types) == False and field.related_model._meta.db_table
                              in self.sharded_tables)):
                        self.sharded_tables.add(model._meta.db_table)

    def db_for_read(self, model, **hints):
        if model._meta.db_table in self.sharded_tables:
            bucket_id = None
            inst = getattr(hints, 'instance', False)
            if inst and inst._state.db:
                return inst._state.db
            if not inst and getattr(hints, 'pk', False):
                u_id = hints['pk']
                bucket_id = id_to_bucket_id(u_id)
            if Sharded64Model in model.mro():
                # map bucket to shard
                bucket_id = getattr(inst, 'bucket_id', False)


            if not bucket_id:
                return None
            shard = bucket_to_shard(bucket_id)
            return SHARDED_DB_PREFIX + str(shard).zfill(3)

        return None

    def db_for_write(self, model, **hints):
        if model._meta.db_table not in self.sharded_tables:
            return None
        if Sharded64Model in model.mro():
            try:
                inst = hints['instance']
            except:
                return None
            bucket_id = getattr(inst, 'bucket_id', False)
            if not bucket_id:
                return None
            shard, new_shard = bucket_to_shard(bucket_id)
            # map bucket to shard
            #                  shard, new_shard)
            if new_shard >= 0: inst.save(using=SHARDED_DB_PREFIX +
                                               str(new_shard).zfill(3))
            return SHARDED_DB_PREFIX + str(shard).zfill(3)

        for field in model._meta.get_fields(include_hidden=True):
            if isinstance(field, (RelatedField, ForeignObjectRel)):
                inst = hints['instance']
                if inst:
                    # TODO
                    u_id = getattr(inst, 'prof_id', False)
                    if not u_id:
                        return None
                    bucket_id = id_to_bucket_id(u_id)
                    shard, new_shard = bucket_to_shard(bucket_id)
                    if new_shard >= 0: inst.save(using=SHARDED_DB_PREFIX +
                                                str(new_shard).zfill(3))
                    print("Saving %s into %s, %s" % (str(inst), shard, new_shard))
                    return SHARDED_DB_PREFIX + str(shard).zfill(3)

        return None

    def allow_relation(self, obj1, obj2, **hints):
        print("allow relation? let's find out")
        if obj1._meta.db_table in self.sharded_tables and obj2._meta.db_table in self.sharded_tables:
            return True
        return None
