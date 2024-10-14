from django.core.management.base import BaseCommand
import multiprocessing
import time
import os
import uuid
from datetime import datetime


class Command(BaseCommand):
    help = "Runs multiple workers with sequential tasks"

    def add_arguments(self, parser):
        parser.add_argument(
            "--workers", type=int, default=4, help="Number of workers to spawn"
        )

    def task(self, task_id, worker_id):
        pid = os.getpid()
        task_uuid=uuid.uuid4()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(f"service_log.log", "w") as f:
            f.write(
                f"Process ID: {pid}, uuid:{task_uuid} , Worker: {worker_id}, Task: {task_id}, Timestamp: {timestamp}\n"
            )

        # Simulate long-running work
        time.sleep(120)  # Run for 1 hour

        with open(f"service_log.log", "w") as f:
            f.write(
                f"Ended Process ID: {pid},uuid:{task_uuid} , Worker: {worker_id}, Task: {task_id}, Timestamp: {timestamp}\n"
            )

    def worker(self, worker_id):
        for task_id in range(5):
            self.stdout.write(
                self.style.SUCCESS(f"Worker {worker_id} starting task {task_id}")
            )
            self.task(task_id, worker_id)
            self.stdout.write(
                self.style.SUCCESS(f"Worker {worker_id} completed task {task_id}")
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Worker {worker_id} has completed all tasks and is now exiting."
            )
        )

    def handle(self, *args, **options):
        num_workers = options["workers"]
        workers = []

        for i in range(num_workers):
            p = multiprocessing.Process(target=self.worker, args=(i,))
            workers.append(p)
            p.start()

        for p in workers:
            p.join()

        self.stdout.write(self.style.SUCCESS("All workers have completed."))
