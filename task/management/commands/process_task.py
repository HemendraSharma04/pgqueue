from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
import os
import time
import random
import signal
import sys
import redis

# Local Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0


class Command(BaseCommand):
    help = "Process tasks with workers that fetch their own tasks"

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=100)

    def __init__(self):
        super().__init__()
        self.running = True

    def check_for_graceful_restart(self):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        restart_id = r.get("restart_id")
        return restart_id is not None

    def process_task(self, task):
        # Simulate a long-running task
        computation_time = random.randint(5, 10)  # 5 to 10 seconds
        time.sleep(computation_time)
        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()

    def signal_handler(self, signum, frame):
        self.stdout.write(
            f"Signal {signum} received. Preparing for graceful shutdown..."
        )
        self.running = False

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        self.stdout.write(f"Starting task processing...")

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        while self.running:
            try:
                # Check if a graceful restart is requested
                if self.check_for_graceful_restart():
                    self.stdout.write("Graceful restart detected. Exiting...")
                    break

                with transaction.atomic():
                    tasks = (
                        Task.objects.filter(status="pending")
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[:batch_size]
                    )
                    task_ids = list(tasks.values_list("id", flat=True))

                    if not task_ids:
                        time.sleep(0.1)  # Short sleep if no tasks found
                        continue

                    Task.objects.filter(id__in=task_ids).update(status="processing")

                # Execute tasks one by one
                for task_id in task_ids:
                    task = Task.objects.get(id=task_id)  # Fetch the task to process
                    self.process_task(task)  # Process the task

            except Exception as e:
                self.stdout.write(f"Error in processing tasks: {str(e)}")
                time.sleep(1)  # Wait a bit before trying again

        self.stdout.write(self.style.SUCCESS("All tasks have been processed."))
