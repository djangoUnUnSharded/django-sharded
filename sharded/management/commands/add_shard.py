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

from django.core.management.commands import migrate
from django.core.management import call_command, BaseCommand
from sharded.db import connections, DEFAULT_DB_ALIAS, shards
from sharded.db.routers import SHARD_DICT, bucket_to_shard, BUCKET_DICT


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

    def handle(self, **options):
        path = options["config_path"]

        # TODO : Read from list
        dbs = readFromList(path)

        # Migrate dbs
        command = self.command_string # actual user-invoked command string
        writeToLog(command)
        # Store to log
        migrate(dbs, command)

        # Rewrite existing dbconfigfile
        rewriteConfig()

def readFromList(path):

    # read from path
    # if failed raise exception
    # else return parsed db object
    return dbs


def migrate(dbs, command):

    # check if existing failed
    if not completed_already(command):
        return

    markMigratingDBs(db)

    # modify logical shards list

    # copy existing tables from shard dbs to new db(s)
    copyAndDelete(dbs) # don't copy all then delete all

    # rebalance
    rebalance(dbs)


def copyBuckets(dbs):


def copyAndDelete():

    for s in logicalShards:



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

def markMigratingDBs(current_db, new_db):

    buckets = SHARD_DICT[current_db]
    for buck in buckets:
        if buck % len(shards) == new_db:
            BUCKET_DICT[buck][1] = new_db
def completed_already(command):


    try:
        migration_attempt = MigrationCommandModel.objects.get(pk=command)
        if migration_attempt.complete:
            return True
        else:
            return False
    except MigrationCommandModel.DoesNotExist:
        return False


def writeToLog():

    # write commands to our log ( logfile or db )

def rewriteConfig():

    # write command

