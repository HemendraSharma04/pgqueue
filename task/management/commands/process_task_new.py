import os
import time
import signal
import logging
import logging.handlers
from django.core.management.base import BaseCommand
import multiprocessing
from django.db import transaction
import subprocess
import sys

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s")
syslog_handler.setFormatter(formatter)
logger.addHandler(syslog_handler)


def django_setup():
    import django

    django.setup()


def execute_task(task_id):
    try:
        process = subprocess.Popen(
            ["nohup", "poetry", "run" ,"python", "manage.py", "run_single_task", str(task_id)],
            stdout=open(f"/tmp/task_{task_id}.out", "w"),
            stderr=open(f"/tmp/task_{task_id}.err", "w"),
            preexec_fn=os.setpgrp,
            close_fds=True,
            start_new_session=True,
        )
        logger.info(f"Started task {task_id} with PID {process.pid}")
        return process.pid
    except Exception as e:
        logger.error(f"Failed to start task {task_id}: {str(e)}")
        return None


def process_task(task_id):
    """Process a single task."""

    os.setsid()
    django_setup()
    from django.utils import timezone
    from task.models import Task

    try:
        print("fetched the task!!!!!!!")
        task = Task.objects.get(id=task_id)
        computation_time = 2  # Simulate task time
        for i in range(5):
            logger.info(f"Processing task {task_id} iteration {i}")
            print(f"Processing task {task_id} iteration {i}")
            time.sleep(2)

        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = os.getpid()
        task.save()
        print(f"Task {task_id} completed.")
    except Exception as e:
        logger.error(f"Error processing task {task_id}: {str(e)}")


def worker_process(batch_size, total_tasks, shutdown_flag, worker_id):
    """Worker process that fetches tasks in batches and processes them one at a time."""
    django_setup()
    from task.models import Task
    from worker.models import Worker

    worker = Worker.objects.create(name=f"Worker-{worker_id}", status="active")
    print(f"Worker {worker.name} (PID: {os.getpid()}) started...")

    processed_tasks = 0
    current_batch = []

    while not shutdown_flag.is_set() and processed_tasks < total_tasks:
        try:
            # Fetch tasks in batches if the current batch is empty
            if not current_batch:
                with transaction.atomic():
                    print("Fetching tasks...")
                    tasks = (
                        Task.objects.filter(status="pending")
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[:batch_size]
                    )

                    if not tasks:
                        time.sleep(0.5)
                        continue

                    task_ids = [task.id for task in tasks]
                    Task.objects.filter(id__in=task_ids).update(
                        status="processing", worker=worker
                    )
                    current_batch = task_ids

                print(f"Fetched {len(current_batch)} tasks. Starting processing...")

            # Process tasks one by one
            for task_id in current_batch:
                if shutdown_flag.is_set():
                    print(f"Worker {worker.name}: Graceful shutdown in progress...")
                    break

                print(f"Processing task {task_id}")
                process = subprocess.Popen(
                    ["nohup", "python", "manage.py", "run_single_task", str(task_id)],
                    start_new_session=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    preexec_fn=os.setpgrp,
                    close_fds=True,
                )

                print(f"Started task {task_id} with PID {process.pid}")

                # Wait for the process to complete
                if not shutdown_flag.is_set(): 
                    stdout, stderr = process.communicate()

                    if process.returncode == 0:
                        print(f"Task {task_id} completed successfully.")
                        if stdout:
                            print(f"Task output: {stdout.decode().strip()}")
                    else:
                        print(
                            f"Task {task_id} failed with return code {process.returncode}"
                        )
                        if stderr:
                            print(f"Task error: {stderr.decode().strip()}")

                    processed_tasks += 1
                    print(f"Processed {processed_tasks}/{total_tasks} tasks.")
                else:
                    print("shutting down~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

                if shutdown_flag.is_set() or processed_tasks >= total_tasks:
                    break

            # Clear the batch after processing all tasks in it
            current_batch = []

        except Exception as e:
            logger.error(f"Error in worker {worker.name}: {str(e)}")
            time.sleep(1)

    print(f"Worker {worker.name} shutting down...")
    worker.status = "inactive"
    worker.save()


# class Command(BaseCommand):
#     """Django management command to run worker processes."""

#     help = (
#         "Process tasks with multiple workers, fetching and executing them in batches."
#     )

#     def add_arguments(self, parser):
#         parser.add_argument(
#             "--workers", type=int, default=1, help="Number of worker processes."
#         )
#         parser.add_argument(
#             "--batch-size", type=int, default=5, help="Number of tasks per batch."
#         )
#         parser.add_argument(
#             "--total-tasks", type=int, default=20, help="Total tasks to process."
#         )

#     def handle(self, *args, **options):
#         num_workers = options["workers"]
#         batch_size = options["batch_size"]
#         total_tasks = options["total_tasks"]

#         # Use Manager for cross-process synchronization
#         with multiprocessing.get_context("spawn").Manager() as manager:
#             shutdown_flag = manager.Event()
#             processes = []

#             print(f"Starting {num_workers} workers...")

#             for i in range(num_workers):
#                 p = multiprocessing.get_context("spawn").Process(
#                     target=worker_process,
#                     args=(batch_size, total_tasks, shutdown_flag, i),
#                 )
#                 p.start()
#                 processes.append(p)

#             def signal_handler(signum, frame):
#                 print("Received shutdown signal. Stopping workers...")
#                 shutdown_flag.set()

#             signal.signal(signal.SIGTERM, signal_handler)
#             signal.signal(signal.SIGINT, signal_handler)

#             try:
#                 for p in processes:
#                     p.join()
#             except KeyboardInterrupt:
#                 print("Received keyboard interrupt. Stopping workers...")
#                 shutdown_flag.set()

#             print("All workers have completed.")


class Command(BaseCommand):
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

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Stopping workers...")
            shutdown_flag.set()
            # Give workers a moment to start shutting down
            time.sleep(2)
            # Exit the main process
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        logger.info(f"Starting {num_workers} workers...")

        for i in range(num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(batch_size, total_tasks, shutdown_flag, i),
            )
            p.start()
            processes.append(p)

        try:
            # Wait for workers to complete or for a shutdown signal
            while any(p.is_alive() for p in processes):
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Stopping workers...")
            shutdown_flag.set()

        logger.info("Main process exiting.")
