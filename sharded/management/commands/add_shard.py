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
import sys

from django.core.management.commands import migrate
from django.core.management import call_command, BaseCommand
from sharded.db import connections, DEFAULT_DB_ALIAS, shards
from sharded.db.routers import SHARD_DICT, bucket_to_shard, BUCKET_DICT, NUM_BUCKETS


class Command(BaseCommand):
    help = "Adds new shard to shard pool"

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)

        parser.add_argument("--alias", action="store", help="DB alias", required=True)
        parser.add_argument("--name", action="store", help="DB name", required=True)
        parser.add_argument("--user", action="store", help="DB user", required=True)
        parser.add_argument("--password", action="store", help="DB password")
        parser.add_argument("--host", action="store", help="DB host", required=True)
        parser.add_argument("--port", action="store", help="DB port")

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
        connections.databases[shard['alias']] = {
            'ENGINE': shard['engine'] if shard['engine'] else shards[0]['ENGINE'],
            'NAME': shard['name'],
            'USER': shard['user'],
            'PASSWORD': shard['password'],
            'HOST': shard['host'],
            'PORT': shard['port'],
        }

        # mark in-migration buckets
        markMigratingDBs()

        # copy existing tables from shard dbs to new db(s)
        copyAndDelete()  # don't copy all then delete all

        # rebalance
        rebalance(dbs)

        demarkMigratingDBs()

        # Rewrite existing dbconfigfile
        rewriteConfig()


def writeToLog(command):


    m = MigrationCommandModel(command, completed=False)
    m.save()


def copy():

    shard_count = len(shards)
    buckets = [i for i in range(0, NUM_BUCKETS) if i % shard_count == 0]

    # get all models that are sharded
    # for each model, get all rows
    # filter by those with bucket_id %

def rebalance(dbs):
    # determine which from old to delete
    determineDeletions(currentDB)


def determineDeletions(currentDB):
    buckets = []

    # for each table in db
    for db in shardableDBs:
        rows.append(determineBucketsForDeletion(table))

    deleteRows(rows)


def determineBucketsForDeletion(currentDB):
    for b in buckets:

        # somewhere where logical shard code exists, existing in memory map, which will be modified, 
        # so this func doens't take args
        db_name = db.bucketMappingFunction(b)
        # check if in right db
        if db_name is not currentDB.name:
            markDeletion(bucket, currentDB)


def markDeletion(bucket, currentDB):
    self.deletionsQueue.append(bucket, currentDB)


def deleteBuckets():
    for b in self.deletionsQueue:
        deleteBucket()


def deleteBucket():


# delete the bucket

def markMigratingDBs():
    shard_count = len(shards)
    buckets = [i for i in range(0, NUM_BUCKETS) if i % shard_count == 0]
    for buck in buckets:
        BUCKET_DICT[buck][1] = shards[len(shards)-1]


def completed_already(command):
    try:
        migration_attempt = MigrationCommandModel.objects.get(pk=command)
        if migration_attempt.complete:
            return True
        else:
            return False
    except MigrationCommandModel.DoesNotExist:
        return False


def rewriteConfig():

# write command
