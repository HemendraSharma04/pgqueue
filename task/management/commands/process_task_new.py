from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
from worker.models import Worker
import os
import time
import random
import signal
import sys
import subprocess
import multiprocessing


class Command(BaseCommand):
    help = "Process tasks with workers that fetch their own tasks"

    def add_arguments(self, parser):
        parser.add_argument("--workers", type=int, default=4)
        parser.add_argument("--batch-size", type=int, default=100)

    def create_worker(self):
        worker = Worker.objects.create(name=f"Worker-{os.getpid()}", status="active")
        return worker

    def process_task(self, task_id):
        task = Task.objects.get(id=task_id)
        computation_time = random.randint(5, 10)  # 5 to 10 seconds
        time.sleep(computation_time)
        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()

    def execute_task_in_background(self, task_id):
        # This will be executed as a separate process
        subprocess.Popen(
            ["python", "manage.py", "process_single_task", "--task_id", str(task_id)]
        )

    def worker_process(self, batch_size):
        worker = self.create_worker()
        self.stdout.write(f"Worker {worker.name} (PID: {os.getpid()}) started...")

        def sigterm_handler(signum, frame):
            self.stdout.write(
                f"Worker {worker.name} (PID: {os.getpid()}) received SIGTERM. Exiting gracefully..."
            )
            worker.status = "inactive"
            worker.save()
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)

        while True:
            try:
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

                    Task.objects.filter(id__in=task_ids).update(
                        status="processing", worker=worker
                    )

                # Start each task in a background process
                for task_id in task_ids:
                    self.execute_task_in_background(task_id)

            except Exception as e:
                self.stdout.write(f"Error in worker {worker.name}: {str(e)}")
                time.sleep(1)  # Wait a bit before trying again

    def handle(self, *args, **options):
        num_workers = options["workers"]
        batch_size = options["batch_size"]

        self.stdout.write(f"Starting {num_workers} workers...")

        # Start worker processes
        processes = []
        for _ in range(num_workers):
            p = multiprocessing.Process(target=self.worker_process, args=(batch_size,))
            p.start()
            processes.append(p)

        # Set up signal handler for the main process
        def sigterm_handler(signum, frame):
            self.stdout.write("Main process received SIGTERM. Shutting down workers...")
            for p in processes:
                p.terminate()
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)

        # Main process waits for worker processes to complete
        for p in processes:
            p.join()

        self.stdout.write(self.style.SUCCESS("All workers have completed."))


class ProcessSingleTaskCommand(BaseCommand):
    help = "Process a single task"

    def add_arguments(self, parser):
        parser.add_argument("--task_id", type=int, required=True)

    def handle(self, *args, **options):
        task_id = options["task_id"]
        Command().process_task(task_id)
