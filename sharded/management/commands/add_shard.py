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
import json
import os
import sys
import time

from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.core.management.commands import migrate, makemigrations
from django.core.management import call_command, BaseCommand
from sharded.db import connections, DEFAULT_DB_ALIAS, shards, SHARDED_DB_PREFIX
from sharded.db.models import ShardedModel
from sharded.db.routers import SHARD_DICT, bucket_to_shard, BUCKET_DICT, NUM_BUCKETS, create_bucket_dict
from sharded.models import MigrationCommand
from django.conf import settings


class Command(BaseCommand):
    help = "Adds new shard to shard pool"

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)

        # parser.add_argument("--alias", action="store", help="DB alias", required=True)
        parser.add_argument("--name", action="store", help="DB name", required=True)
        parser.add_argument("--user", action="store", help="DB user", required=True)
        parser.add_argument("--password", action="store", help="DB password")
        parser.add_argument("--host", action="store", help="DB host", required=True)
        parser.add_argument("--port", action="store", help="DB port", required=True)
        parser.add_argument("--engine", action="store", help="DB engine", required=True)

    def handle(self, **shard):

        # Migrate dbs
        command = ' '.join(sys.argv[1:])  # actual user-invoked command string
        # check if existing failed
        if not completed_already(command):
            # Store to log
            writeToLog(command)
        else:
            return

        # modify connections list
        shard_name = SHARDED_DB_PREFIX + str(len(shards)+1).zfill(3)
        shard_entry = {
            'ENGINE': shard.get('engine') if shard.get('engine') else connections.databases['default']['ENGINE'],
            'NAME': shard['name'],
            'USER': shard['user'],
            'PASSWORD': shard['password'],
            'HOST': shard['host'],
            'PORT': shard['port'],
        }
        connections.databases[shard_name] = shard_entry
        shards.append(shard_name)

        call_command('makemigrations')
        call_command('migrate', '--all')


        # mark in-migration buckets
        to_migrate = markMigratingDBs()

        # copy existing tables from shard dbs to new db(s)
        to_delete, bucket_counts = copyAndGetDeletions(to_migrate, shard_name)  # don't copy all then delete all

        # delete old
        delete(to_delete, bucket_counts)

        # update bucket dict
        updateBucketDict()

        # Rewrite existing dbconfigfile
        rewriteConfig(shard_entry, shard_name)

        # commit to migrationCommand table
        commitMigrationCommand(command)


def writeToLog(command):
    m = MigrationCommand(command, complete=False)
    m.save()

def commitMigrationCommand(command):
    m = MigrationCommand(command, complete=True)
    m.save()

def updateBucketDict():
    create_bucket_dict()

def copyAndGetDeletions(shard_migrations, to_shard):
    #TODO generify
    mods = list(apps.get_app_config('micro').get_models())
    x = lambda m: ShardedModel in m.mro()
    shmods = list(filter(x, mods))
    to_delete = {}
    bucket_counts = {}
    for model in shmods:
        for shard, bucks in shard_migrations.items():
            objs = []
            shard_to_query = SHARDED_DB_PREFIX + str(shard).zfill(3)
            matches = model.objects.using(shard_to_query).filter(bucket_id__in=bucks)
            for instance in matches:
                rel_objs = []
                for rel_obj in instance._meta.related_objects:
                    rel_name = rel_obj.name
                    rel_set = rel_name + '_set'
                    rels = getattr(instance, rel_set, False)
                    if rels:
                        rel_objs.extend(rels.all())


                # profile.save(using=to_shard)
                objs.append(instance)
                objs.extend(rel_objs)
                to_delete[shard_to_query] = objs
                # bucket_counts[instance.bucket_id] += 1 + len(rel_objs)
            for obj in objs:
                obj.save(using=to_shard)
    return to_delete, bucket_counts



def delete(to_delete, bucket_counts):
    for shard, objs in to_delete.items():
        for obj in objs:
            obj.delete(using=shard)


def markMigratingDBs():
    shard_count = len(shards)
    to_migrate = {}
    buckets = [i for i in range(1, NUM_BUCKETS) if i % shard_count == 0]
    for buck in buckets:
        BUCKET_DICT[buck][1] = shards[len(shards)-1]
        if BUCKET_DICT[buck][0] in to_migrate:
            to_migrate[BUCKET_DICT[buck][0]].append(buck)
        else:
            to_migrate[BUCKET_DICT[buck][0]] = [buck]
    return to_migrate


def completed_already(command):
    try:
        migration_attempt = MigrationCommand.objects.get(pk=command)
        if migration_attempt.complete:
            return True
        else:
            return False
    except MigrationCommand.DoesNotExist:
        return False


def rewriteConfig(shard_entry, shard_name):
    # get db_settings file
    DB_FILE = getattr(settings, 'DB_FILE', None)
    if DB_FILE is None:
        raise ImproperlyConfigured(
                "The DB settings file is not properly configuired")
    with open(DB_FILE, 'r') as db_data:
        DATABASES = json.load(db_data)
    DATABASES[shard_name] = shard_entry
    tmp_filename = DB_FILE + str(time.time())
    dup = open(tmp_filename, 'wb')
    json.dump(DATABASES, dup)

    with open(DB_FILE, 'w') as db_file:
        db_file.write(json.dumps(DATABASES))

    if os.path.exists(tmp_filename):
        os.remove(tmp_filename)

    # write to og file
    # check if file was safely written
    # delete dupe

