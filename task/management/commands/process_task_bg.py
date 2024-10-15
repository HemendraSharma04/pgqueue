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
import multiprocessing


class TaskProcess(multiprocessing.Process):
    """Process to handle individual task execution."""

    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id

    def run(self):
        try:
            task = Task.objects.get(id=self.task_id)
            computation_time = random.randint(5, 10)  # Simulate task execution time
            time.sleep(computation_time)

            # Update task status after completion
            task.result = f"Computed for {computation_time} seconds"
            task.status = "completed"
            task.completed_at = timezone.now()
            task.counter = 1
            task.save()

        except Exception as e:
            print(f"Error processing task {self.task_id}: {e}", flush=True)


class Command(BaseCommand):
    help = "Process tasks with workers fetching batches but executing one at a time."

    def add_arguments(self, parser):
        parser.add_argument("--workers", type=int, default=4)
        parser.add_argument("--batch-size", type=int, default=10)

    def create_worker(self):
        """Create and return a new worker."""
        return Worker.objects.create(name=f"Worker-{os.getpid()}", status="active")

    def worker_process(self, batch_size, shutdown_flag):
        """Worker function to fetch tasks in batches and run them one at a time."""
        worker = self.create_worker()
        self.stdout.write(f"Worker {worker.name} (PID: {os.getpid()}) started...")

        def sigterm_handler(signum, frame):
            self.stdout.write(
                f"Worker {worker.name} (PID: {os.getpid()}) received SIGTERM. Exiting gracefully..."
            )
            worker.status = "inactive"
            worker.save()
            shutdown_flag.set()
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)

        while not shutdown_flag.is_set():
            try:
                # Fetch a batch of tasks atomically
                with transaction.atomic():
                    tasks = (
                        Task.objects.filter(status="pending")
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[:batch_size]
                    )

                    if not tasks:
                        time.sleep(0.5)  # No tasks found, short sleep
                        continue

                    # Mark tasks as processing and associate them with the worker
                    Task.objects.filter(id__in=[task.id for task in tasks]).update(
                        status="processing", worker=worker
                    )

                # Execute tasks one by one in the batch
                for task in tasks:
                    if shutdown_flag.is_set():
                        break  # Stop processing new tasks if shutdown signal is received

                    # Start the task in a separate process
                    p = TaskProcess(task_id=task.id)
                    p.start()

                    # Wait for the task to finish or for the shutdown signal
                    while p.is_alive():
                        if shutdown_flag.is_set():
                            self.stdout.write(
                                f"Worker {worker.name}: Graceful shutdown in progress..."
                            )
                            break
                        time.sleep(0.5)  # Poll every 0.5 seconds

                    p.join()  # Ensure the process completes

            except Exception as e:
                self.stdout.write(f"Error in worker {worker.name}: {str(e)}")
                time.sleep(1)  # Wait a bit before trying again

        self.stdout.write(f"Worker {worker.name} shutting down...")

    def handle(self, *args, **options):
        """Main entry point to manage multiple workers."""
        num_workers = options["workers"]
        batch_size = options["batch_size"]

        shutdown_flag = multiprocessing.Event()
        processes = []

        self.stdout.write(f"Starting {num_workers} workers...")

        # Start worker processes
        for _ in range(num_workers):
            p = multiprocessing.Process(
                target=self.worker_process, args=(batch_size, shutdown_flag)
            )
            p.start()
            processes.append(p)

        def sigterm_handler(signum, frame):
            self.stdout.write("Main process received SIGTERM. Shutting down workers...")
            shutdown_flag.set()

        # Handle SIGTERM for graceful shutdown
        signal.signal(signal.SIGTERM, sigterm_handler)

        # Wait for all workers to complete
        for p in processes:
            p.join()

        self.stdout.write(self.style.SUCCESS("All workers have completed."))

