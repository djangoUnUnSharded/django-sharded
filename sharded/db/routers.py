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
from sharded.db.models import ShardedModel, Sharded64Model
from sharded.exceptions import ShardCouldNotBeDetermined

NUM_BUCKETS = getattr(settings, 'NUM_BUCKETS', 2048)

class ShardedRouter(object):
    def __init__(self):
        self.sharded_tables = set() # set of sharded tables
        len_sharded_tables = -1
        while len_sharded_tables != len(self.sharded_tables):
            len_sharded_tables = len(self.sharded_tables)
            # for every model that is NOT in the set of sharded tables
            for model in filter(lambda m: m._meta.db_table not in self.sharded_tables, apps.get_models(include_auto_created=True, include_deferred=True)):
                # for every field that is in those models
                # for field in model._meta.get_fields(include_hidden=True):
                # if those fields are an instance of the sharded fields
                if isinstance(model, models.Sharded64Model) or \
                    (isinstance(model, (RelatedField,ForeignObjectRel)) and \
                     (isinstance(model.related_model, six.string_types)==False and model.related_model._meta.db_table
                      in self.sharded_tables)):
                    self.sharded_tables.add(model._meta.db_table)



    # def db_for_read(self, model, **hints):
    #     if isinstance(model, Sharded64Model):
    #         # map bucket to shard
    #         bucket_id = model.id
    #         shard = bucket_to_shard(bucket_id)
    #         return shard
    #     return None
    #
    #
    # def db_for_write(self, model, **hints):
    #     if isinstance(model, Sharded64Model):
    #         bucket_id = model.bucket_id
    #         shard, new_shard = bucket_to_shard(bucket_id)
    #         model.save(using=new_shard)
    #         return shard
    #     return None


    def allow_relation(self, obj1, obj2, **hints):
        if obj1._meta.db_table in self.sharded_tables and obj2._meta.db_table in self.sharded_tables:
            return True
        return None
