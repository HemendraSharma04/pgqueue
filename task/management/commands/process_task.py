from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
from worker.models import Worker
import threading
import time
import concurrent.futures
import socket
from django.db.models import F
import random


class Command(BaseCommand):
    help = "Process tasks with workers that fetch their own tasks"

    def add_arguments(self, parser):
        parser.add_argument("--workers", type=int, default=4)
        parser.add_argument(
            "--runtime",
            type=int,
            default=5,
            help="How long to run the processing in seconds",
        )
        parser.add_argument("--batch-size", type=int, default=100)

    def create_worker(self):
        thread_id = threading.get_ident()
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        worker = Worker.objects.create(
            name=f"Worker-Thread-{thread_id}", status="active", ip=ip
        )
        return worker

    # we can do bulk update here too
    def process_task(self, task):
        computation_time = random.randint(100, 500) / 1000  # 100 to 500 milliseconds
        task.result = f"Computed for {computation_time:.3f} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()

        return computation_time

    def worker_process(self, worker_id, stop_event, batch_size):
        worker = self.create_worker()
        self.stdout.write(f"Created worker: {worker.name} with IP: {worker.ip}")

        try:
            while not stop_event.is_set():
                tasks_processed = 0
                total_computation_time = 0

                with transaction.atomic():
                    tasks = (
                        Task.objects.filter(status="pending", worker__isnull=True)
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[:batch_size]
                    )

                    task_ids = list(tasks.values_list("id", flat=True))

                    if not tasks:
                        time.sleep(0.1)  # Short sleep if no tasks found
                        continue

                    Task.objects.filter(id__in=task_ids).update(
                        status="processing",
                        last_picked_at=timezone.now(),
                        picked_at=F("picked_at") or timezone.now(),
                        worker=worker,
                    )

                # Process tasks
                for task in tasks:
                    if stop_event.is_set():
                        break

                    computation_time = self.process_task(task)
                    total_computation_time += computation_time
                    tasks_processed += 1

                if tasks_processed > 0:
                    self.stdout.write(
                        f"Worker {worker.name} processed {tasks_processed}"
                    )

        finally:
            worker.status = "inactive"
            worker.save()
            self.stdout.write(f"Worker {worker.name} marked as inactive")

    def handle(self, *args, **options):
        num_workers = options["workers"]
        runtime = options["runtime"]
        batch_size = options["batch_size"]

        self.stdout.write(
            f"Starting {num_workers} workers to run for {runtime} seconds..."
        )

        stop_event = threading.Event()

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(self.worker_process, worker_id, stop_event, batch_size)
                for worker_id in range(num_workers)
            ]

            # Wait for the specified runtime
            time.sleep(runtime)

            # Signal all threads to stop
            self.stdout.write("Signaling workers to stop...")
            stop_event.set()

            # Wait for all futures to complete
            self.stdout.write("Waiting for all workers to complete...")
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result(timeout=5)
                except concurrent.futures.TimeoutError:
                    self.stdout.write(
                        self.style.WARNING("A worker did not complete in time")
                    )

        self.stdout.write(self.style.SUCCESS("All workers completed or timed out"))

        # Clean up any remaining "processing" tasks
        with transaction.atomic():
            updated = Task.objects.filter(status="processing").update(
                status="pending", worker=None
            )
            self.stdout.write(
                f"Reset {updated} tasks from 'processing' to 'pending' status"
            )
