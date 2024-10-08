from django.core.management.base import BaseCommand
from django.utils import timezone
from task.models import Task
from task_queue.models import TaskQueue
import random
from django.db import transaction


class Command(BaseCommand):
    help = "Creates 100k sample tasks"

    def handle(self, *args, **options):
        # Assuming you have at least one TaskQueue
        queue = TaskQueue.objects.first()
        if not queue:
           queue = TaskQueue.objects.create(name="default", description="Default queue")

        batch_size = 10000
        num_batches = 100  # This will create 100k records (1000 * 100)

        self.stdout.write("Starting to create tasks...")

        for batch in range(num_batches):
            tasks = []
            for _ in range(batch_size):
                task = Task(
                    queue=queue,
                    status="pending",
                    extra_params={"sample": "data"},
                    visibility_timeout=random.choice([30, 60, 90, 120]),
                )
                tasks.append(task)

            with transaction.atomic():
                Task.objects.bulk_create(tasks)

            self.stdout.write(f"Created batch {batch + 1}/{num_batches}")

        self.stdout.write(self.style.SUCCESS("Successfully created 1000k sample tasks"))
