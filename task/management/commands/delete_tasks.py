from django.core.management.base import BaseCommand
from task.models import Task
from worker.models import Worker
from django.db import transaction


class Command(BaseCommand):
    help = "Resets tasks and deletes all workers"

    def handle(self, *args, **options):
        self.stdout.write("Starting to reset tasks...")

        # Use bulk update to reset task counter and status in a single query
        with transaction.atomic():
            task_updated_count = Task.objects.all().update(counter=0, status="pending")
            worker_deleted_count = Worker.objects.all().delete()[0]

        self.stdout.write(f"Tasks updated: {task_updated_count}")
        self.stdout.write(f"Workers deleted: {worker_deleted_count}")
