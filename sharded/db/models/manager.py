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

from django.db.models.manager import BaseManager
from sharded.db.models.query import ShardedQuerySet

class BaseShardedManager(BaseManager):
    def __iter__(self):
        print("sharded.db.models.manager: iterating over BaseShardedManager")
        return self.all().__iter__()
    
    @classmethod
    def from_queryset(cls, queryset_class, class_name=None):
        return super(BaseShardedManager, cls).from_queryset(queryset_class, class_name=class_name)
#    def get_queryset(self):
#        print("sharded.db.models.manager: mod, db, hints =", self.model, self._db,self._hints)
#        self._hints['prof_id'] = 1
#        return self._queryset_class(self.model, using=self._db, hints=self._hints)


class ShardedManager(BaseShardedManager.from_queryset(ShardedQuerySet)):
    use_for_related_fields = True
