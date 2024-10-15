import os
import time
import signal
import logging
import logging.handlers
from django.core.management.base import BaseCommand
import multiprocessing
from django.db import transaction

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


def process_task(task_id):
    """Process a single task."""
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
    """Worker process that fetches tasks in batches and processes them."""
    django_setup()
    from task.models import Task
    from worker.models import Worker

    worker = Worker.objects.create(name=f"Worker-{worker_id}", status="active")
    print(f"Worker {worker.name} (PID: {os.getpid()}) started...")

    processed_tasks = 0
    current_batch = []

    while not shutdown_flag.is_set() and processed_tasks < total_tasks:
        try:
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

            # Start child processes without waiting for them
            for task_id in current_batch:
                if shutdown_flag.is_set():
                    print(f"Worker {worker.name}: Graceful shutdown in progress...")
                    break

                # Start the task as an independent process
                process = multiprocessing.get_context("spawn").Process(
                    target=process_task, args=(task_id,)
                )
                process.daemon = False  # Allow it to run even if parent exits
                process.start()
                
                while process.is_alive():
                    print("process is alive----------------")
                    time.sleep(2)

                print(f"Started task {task_id} with PID {process.pid}")

                # No need to join, let it run independently in the background
                processed_tasks += 1
                print(f"Processed {processed_tasks}/{total_tasks} tasks.")

            # Clear the batch after dispatching all tasks
            current_batch = []

        except Exception as e:
            logger.error(f"Error in worker {worker.name}: {str(e)}")
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

        # Use Manager for cross-process synchronization
        with multiprocessing.get_context("spawn").Manager() as manager:
            shutdown_flag = manager.Event()
            processes = []

            print(f"Starting {num_workers} workers...")

            for i in range(num_workers):
                p = multiprocessing.get_context("spawn").Process(
                    target=worker_process,
                    args=(batch_size, total_tasks, shutdown_flag, i),
                )
                p.start()
                processes.append(p)

            def signal_handler(signum, frame):
                print("Received shutdown signal. Stopping workers...")
                shutdown_flag.set()

            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)

            try:
                for p in processes:
                    p.join()
            except KeyboardInterrupt:
                print("Received keyboard interrupt. Stopping workers...")
                shutdown_flag.set()

            print("All workers have completed.")
