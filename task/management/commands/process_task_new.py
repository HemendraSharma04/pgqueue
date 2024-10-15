import os
import time
import signal
import logging
import logging.handlers
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
from worker.models import Worker
import multiprocessing

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
# formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s")
# syslog_handler.setFormatter(formatter)
# logger.addHandler(syslog_handler)


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
        task.counter = os.getpid()
        task.save()
        logger.info(f"Task {task_id} completed.")
    except Exception as e:
        logger.error(f"Error processing task {task_id}: {str(e)}")


def worker_process(batch_size, total_tasks, shutdown_flag, worker_id):
    """Worker process that fetches tasks in batches and processes them."""
    import django

    django.setup()

    worker = Worker.objects.create(name=f"Worker-{worker_id}", status="active")
    logger.info(f"Worker {worker.name} (PID: {os.getpid()}) started...")

    processed_tasks = 0
    current_batch = []

    with multiprocessing.get_context("spawn").Pool() as pool:
        while not shutdown_flag.is_set() and processed_tasks < total_tasks:
            try:
                if not current_batch:
                    with transaction.atomic():
                        logger.info("Fetching tasks...")
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

                    logger.info(
                        f"Fetched {len(current_batch)} tasks. Starting processing..."
                    )

                # Process each task in the batch
                for task_id in current_batch:
                    if shutdown_flag.is_set():
                        logger.info(
                            f"Worker {worker.name}: Graceful shutdown in progress..."
                        )
                        break  # Stop processing if shutdown signal is received

                    # Use pool.apply_async to process the task
                    pool.apply_async(process_task, (task_id,))

                    processed_tasks += 1
                    logger.info(f"Processed {processed_tasks}/{total_tasks} tasks.")

                # Clear the current batch after processing all tasks
                current_batch = []

            except Exception as e:
                logger.error(f"Error in worker {worker.name}: {str(e)}")
                time.sleep(1)

    logger.info(f"Worker {worker.name} shutting down...")
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

        logger.info(f"Starting {num_workers} workers...")

        for i in range(num_workers):
            p = multiprocessing.get_context("spawn").Process(
                target=worker_process,
                args=(batch_size, total_tasks, shutdown_flag, i),
            )
            p.start()
            processes.append(p)

        def signal_handler(signum, frame):
            logger.info("Received shutdown signal. Stopping workers...")
            shutdown_flag.set()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Stopping workers...")
            shutdown_flag.set()

        logger.info("All workers have completed.")
