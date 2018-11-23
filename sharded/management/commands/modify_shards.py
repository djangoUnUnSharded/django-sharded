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
from django.core.management import call_command
from sharded.db import connections, DEFAULT_DB_ALIAS, shards

class Command(migrate.Command):
    help = "Updates database schema on default DB and shards. Manages both apps with migrations and those without."
    
    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)
        parser.add_argument("--config-path", action="store_true", default=False, help="Path to db config module")
        # parser.add_argument("--no-initshard", action="store_true", default=False, help="Do not call initshard.")
    
    def handle(self, **options):
        
        # TODO : Read from list
        list = configFile.DATABASES
        dbs = readFromList(list)
        
        # Migrate dbs
        command = self.command_string # actual user-invoked command string
        writeToLog(command)
        # Store to log
        migrate(dbs, command)

        # Rewrite existing dbconfigfile
        rewriteConfig()



def migrate(dbs, command):

    # check if existing faile
    if (failcheck(dbs, command)):
        
        # re run
        rerun()
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

def markMigratingDBs():

    # mark old tables for reads

    # mark both tables for writes


def failcheck(dbs, command):


def writeToLog():

    # write commands to our log ( logfile or db )

def rewriteConfig():

    # write command

