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

from django.apps import apps
from django.core.management.commands import migrate, makemigrations
from django.core.management import call_command, BaseCommand
from sharded.db import connections, DEFAULT_DB_ALIAS, shards, SHARDED_DB_PREFIX
from sharded.db.models import ShardedModel
from sharded.db.routers import SHARD_DICT, bucket_to_shard, BUCKET_DICT, NUM_BUCKETS
from sharded.models import MigrationCommandModel


class Command(BaseCommand):
    help = "Adds new shard to shard pool"

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)

        # parser.add_argument("--alias", action="store", help="DB alias", required=True)
        parser.add_argument("--name", action="store", help="DB name", required=True)
        parser.add_argument("--user", action="store", help="DB user", required=True)
        parser.add_argument("--password", action="store", help="DB password")
        parser.add_argument("--host", action="store", help="DB host", required=True)
        parser.add_argument("--port", action="store", help="DB port")

    def handle(self, **shard):

        # Migrate dbs
        command = ' '.join(sys.argv[1:])  # actual user-invoked command string
        # check if existing failed
        # if not completed_already(command):
        #     # Store to log
        #     writeToLog(command)
        # else:
        #     return

        # modify connections list
        shard_num = SHARDED_DB_PREFIX + str(len(shards)+1).zfill(3)
        connections.databases[shard_num] = {
            'ENGINE': shard.get('engine') if shard.get('engine') else connections.databases['default']['ENGINE'],
            'NAME': shard['name'],
            'USER': shard['user'],
            'PASSWORD': shard['password'],
            'HOST': shard['host'],
            'PORT': shard['port'],
        }
        shards.append(shard_num)

        call_command('makemigrations')
        call_command('migrate', '--no-initshard', '--all')




        # mark in-migration buckets
        to_migrate = markMigratingDBs()

        # copy existing tables from shard dbs to new db(s)
        copyAndDelete(to_migrate, shard_num)  # don't copy all then delete all

        # rebalance
        rebalance(dbs)

        demarkMigratingDBs()

        # Rewrite existing dbconfigfile
        rewriteConfig()


def writeToLog(command):
    m = MigrationCommandModel(command, completed=False)
    m.save()

def copyAndDelete(shard_migrations, to_shard):
    # objs = []
    # ShardedModel.objects.all()
    # for shard, bucks in shard_migrations:

    mods = list(apps.get_app_config('micro').get_models())
    x = lambda m: ShardedModel in m.mro()
    shmods = list(filter(x, mods))
    for model in shmods:
        objs = []
        for shard, bucks in shard_migrations.items():
            shard_to_query = SHARDED_DB_PREFIX + str(shard).zfill(3)
            matches = model.objects.using(shard_to_query).filter(bucket_id__in=bucks)
            for profile in matches:
                rel_objs = []
                for rel_obj in profile._meta.related_objects:
                    rel_name = rel_obj.name
                    rel_set = rel_name + '_set'
                    rels = getattr(profile, rel_set, False)
                    if rels:
                        rel_objs.extend(rels.all())



                profile.save(using=to_shard)
            # for i in range(start, count, size):
            #     print i,
            #     sys.stdout.flush()
            #     original_data = model.objects.using('mysql').all()[i:i + size]
            #     original_data_json = serializers.serialize("json", original_data)
            #
            # for field in model._meta.get_fields(include_hidden=true):
            #     if             new_data = serializers.deserialize("json", original_data_json,
            #                                        using='default')
            #     for n in new_data:
            #         new_data.save(using='default')



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
    pass


# delete the bucket

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
        migration_attempt = MigrationCommandModel.objects.get(pk=command)
        if migration_attempt.complete:
            return True
        else:
            return False
    except MigrationCommandModel.DoesNotExist:
        return False


def rewriteConfig():
    pass

# # write command
# look at all models in app, we get all models that inherit from sharded. for a given shard we are moving,
#     for each model in shardedmodels:
#         m.getallshardedobjs on shard 1
#         filtered = filter(m, hasBucketIds in to_migrate)
#         for each in stuff: