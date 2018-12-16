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

from itertools import chain
from django.db.models import query
from sharded.db import shards
from sharded.exceptions import ShardCouldNotBeDetermined


class ShardedQuerySetMixin(object):
    connections = shards

    def iterator(self):
        print("sharded.db.models.query: iterating over ShardedQuerySetMixin")
        try:
            self._db = self.db
            return super(ShardedQuerySetMixin, self).iterator()
        except ShardCouldNotBeDetermined:
            # return chain(*[self._clone(_db=cnxn).iterator() for cnxn in self.connections])
            iters = None
            iters_arr = []
            for shard in self.connections:
                clone = self._clone(_db=shard)
                clone_iter = clone.iterator()
                iters_arr.append(clone_iter)
            iters = chain(*iters_arr)
            return iters


class ShardedQuerySet(ShardedQuerySetMixin, query.QuerySet):
    def _filter_or_exclude(self, negate, *args, **kwargs):
        self._add_hints(**kwargs)
        return super(ShardedQuerySet, self)._filter_or_exclude(negate, *args, **kwargs)

    def count(self):
        if self._result_cache is not None:
            return super(ShardedQuerySet, self).count()
        else:
            return sum([self.query.get_count(using=cnxn) for cnxn in self.connections])

    def values(self, *fields):
        clone = super(ShardedQuerySet, self)._values(*fields)
        clone._iterable_class = ShardedValuesIterable
        return clone

    def values_list(self, *fields, **kwargs):
        clone = super(ShardedQuerySet, self).values_list(*fields, **kwargs)
        clone._iterable_class = ShardedFlatValuesListIterable if kwargs.get('flat', False) else ShardedValuesListIterable
        return clone


class ShardedValuesIterable(ShardedQuerySetMixin, query.ValuesIterable):
    pass


class ShardedValuesListIterable(ShardedQuerySetMixin, query.ValuesListIterable):
    pass


class ShardedFlatValuesListIterable(ShardedQuerySetMixin, query.FlatValuesListIterable):
    pass
