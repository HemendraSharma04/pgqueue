import os
import time
import signal
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
from worker.models import Worker
import multiprocessing


def process_task(task_id):
    """Process a single task."""
    import django

    django.setup()

    try:
        task = Task.objects.get(id=task_id)
        computation_time = 40  # Simulate task time
        time.sleep(computation_time)

        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()
        print(f"Task {task_id} completed.")
    except Exception as e:
        print(f"Error processing task {task_id}: {str(e)}")


def worker_process(batch_size, total_tasks, shutdown_flag, worker_id):
    """Worker process that fetches tasks in batches and processes them."""
    import django

    django.setup()

    worker = Worker.objects.create(name=f"Worker-{worker_id}", status="active")
    print(f"Worker {worker.name} (PID: {os.getpid()}) started...")

    processed_tasks = 0

    while not shutdown_flag.is_set() and processed_tasks < total_tasks:
        try:
            # Fetch a batch of tasks
            with transaction.atomic():
                tasks = (
                    Task.objects.filter(status="pending")
                    .select_for_update(skip_locked=True)
                    .order_by("created_at")[:batch_size]
                )

                if not tasks:
                    time.sleep(0.5)
                    continue

                Task.objects.filter(id__in=[task.id for task in tasks]).update(
                    status="processing", worker=worker
                )

            print(f"Fetched {len(tasks)} tasks. Starting processing...")

            # Process each task in the batch
            for task in tasks:
                if shutdown_flag.is_set():
                    print(f"Worker {worker.name}: Graceful shutdown in progress...")
                    break  # Stop processing if shutdown signal is received

                # Fork a new process to handle the task
                pid = os.fork()
                if pid == 0:  # Child process
                    try:
                        process_task(task.id)
                    finally:
                        os._exit(0)  # Exit the child process
                else:  # Parent process
                    # Wait for the child process to finish
                    pid, status = os.waitpid(pid, 0)
                    if os.WIFEXITED(status) and os.WEXITSTATUS(status) != 0:
                        print(f"Child process {pid} exited with error.")

                if shutdown_flag.is_set():
                    print(
                        f"Worker {worker.name}: Stopped processing due to shutdown signal."
                    )
                    break

                processed_tasks += 1
                print(f"Processed {processed_tasks}/{total_tasks} tasks.")

        except Exception as e:
            print(f"Error in worker {worker.name}: {str(e)}")
            time.sleep(1)

    print(f"Worker {worker.name} shutting down...")
    worker.status = "inactive"
    worker.save()


class Command(BaseCommand):
    """Django management command to run worker processes."""

    help = (
        "Process tasks with multiple workers, fetching and executing them in batches."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--workers", type=int, default=1, help="Number of worker processes."
        )
        parser.add_argument(
            "--batch-size", type=int, default=5, help="Number of tasks per batch."
        )
        parser.add_argument(
            "--total-tasks", type=int, default=20, help="Total tasks to process."
        )

    def handle(self, *args, **options):
        num_workers = options["workers"]
        batch_size = options["batch_size"]
        total_tasks = options["total_tasks"]

        shutdown_flag = multiprocessing.Event()
        processes = []

        print(f"Starting {num_workers} workers...")

        for i in range(num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(batch_size, total_tasks, shutdown_flag, i),
            )
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        print("All workers have completed.")
