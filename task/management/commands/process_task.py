from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
from worker.models import Worker
import time
import socket
import random


class Command(BaseCommand):
    help = "Process tasks with a single worker that fetches its own tasks"

    def add_arguments(self, parser):
        parser.add_argument(
            "--runtime",
            type=int,
            default=5,
            help="How long to run the processing in seconds",
        )
        parser.add_argument("--batch-size", type=int, default=100)

    def create_worker(self):
        thread_id = (
            1  # Since we're running single-threaded, we can use a static thread ID
        )
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        worker = Worker.objects.create(
            name=f"Worker-Thread-{thread_id}", status="active", ip=ip
        )
        return worker

    def process_task(self, task):
        computation_time = random.randint(100, 500) / 1000  # 100 to 500 milliseconds
        time.sleep(computation_time)  # Uncomment this line to simulate processing time

        task.result = f"Computed for {computation_time:.3f} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()

        return computation_time

    def handle(self, *args, **options):
        runtime = options["runtime"]
        batch_size = options["batch_size"]

        self.stdout.write(f"Starting a single worker to run for {runtime} seconds...")

        stop_time = time.time() + runtime
        worker = self.create_worker()
        self.stdout.write(f"Created worker: {worker.name} with IP: {worker.ip}")

        try:
            while time.time() < stop_time:
                tasks_processed = 0
                total_computation_time = 0

                with transaction.atomic():
                    tasks = (
                        Task.objects.filter(status="pending", worker__isnull=True)
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[:batch_size]
                    )

                    if not tasks:
                        time.sleep(0.1)  # Short sleep if no tasks found
                        continue

                    for task in tasks:
                        task.status = "processing"
                        task.last_picked_at = timezone.now()
                        task.picked_at = task.picked_at or timezone.now()
                        task.worker = worker
                        task.save()

                        computation_time = self.process_task(task)
                        total_computation_time += computation_time
                        tasks_processed += 1

                if tasks_processed > 0:
                    self.stdout.write(
                        f"Worker {worker.name} processed {tasks_processed} tasks."
                    )

        finally:
            worker.status = "inactive"
            worker.save()
            self.stdout.write(f"Worker {worker.name} marked as inactive")

        # Clean up any remaining "processing" tasks
        with transaction.atomic():
            updated = Task.objects.filter(status="processing").update(
                status="pending", worker=None
            )
            self.stdout.write(
                f"Reset {updated} tasks from 'processing' to 'pending' status"
            )
