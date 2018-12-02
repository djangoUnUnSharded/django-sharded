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

# TODO : see what each thing is doing and move to models


import re

from django.db.backends.postgresql_psycopg2.base import *
from django.db.backends.postgresql_psycopg2.base import DatabaseSchemaEditor as PostgresDatabaseSchemaEditor
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as PostgresDatabaseWrapper



class DatabaseSchemaEditor(PostgresDatabaseSchemaEditor):
    """
    function takes a db field and determines if it is an instance of one of the Sharded model types
    return: true if field is an instance of the Sharded32Field or Sharded64Field
    """
    def skip_default(self, field):
        from sharded.db import models
        return isinstance(field, (models.Sharded32Field, models.Sharded64Field))


# what does this class
class DatabaseWrapper(PostgresDatabaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor

    """
    @cached_property caches the result of the function so we don't
    have to reconnect to the db every time the function is called
    function 
    """
    @cached_property
    def shard_id(self):
        from sharded.db import SHARDED_DB_PREFIX
        # regex matches self.alias for shard_id
        shard_id = re.match(SHARDED_DB_PREFIX + "(\d{1,3})", self.alias) # what is self.alias
        # if not found, return 0, else return the number of the shard
        return 0 if not shard_id else int(shard_id.group(1))
