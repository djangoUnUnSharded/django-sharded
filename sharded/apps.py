from django.apps import AppConfig


class DefaultConfig(AppConfig):
    name = 'sharded'
    verbose_name = "Django Sharded"

    def ready(self):
        from sharded.db.routers import create_bucket_dict
        create_bucket_dict()
