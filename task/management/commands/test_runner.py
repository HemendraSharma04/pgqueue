import os
import time
import signal
import logging
import logging.handlers
from django.core.management.base import BaseCommand
import multiprocessing
from django.db import transaction
import subprocess
from django.utils import timezone

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
            [
                "nohup",
                "poetry",
                "run",
                "python",
                "manage.py",
                "run_single_task",
                str(task_id),
            ],
            stdout=open(f"/tmp/task_{task_id}.out", "w"),
            stderr=open(f"/tmp/task_{task_id}.err", "w"),
            preexec_fn=os.setpgrp,
            close_fds=True,
            start_new_session=True,
        )
        logger.info(f"Started task {task_id} with PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Failed to start task {task_id}: {str(e)}")
        return None


def worker_process(batch_size, shutdown_flag, worker_id):
    """Worker process that fetches and executes tasks one by one."""
    django_setup()
    from task.models import Task
    from worker.models import Worker

    worker = Worker.objects.create(name=f"Worker-{worker_id}", status="active")
    logger.info(f"Worker {worker.name} (PID: {os.getpid()}) started...")

    while not shutdown_flag.is_set():
        try:
            with transaction.atomic():
                tasks = (
                    Task.objects.filter(status="pending")
                    .select_for_update(skip_locked=True)
                    .order_by("created_at")[:batch_size]
                )

                if not tasks:
                    time.sleep(1)
                    continue

                task_ids = [task.id for task in tasks]
                Task.objects.filter(id__in=task_ids).update(
                    status="processing",
                    worker=worker,
                    picked_at=timezone.now(),
                    last_picked_at=timezone.now(),
                )

            # Execute tasks one by one
            for task_id in task_ids:
                if shutdown_flag.is_set():
                    logger.info(f"Shutdown flag set. Skipping task {task_id}.")
                    break  # Exit the loop if shutdown flag is set

                process = execute_task(task_id)
                if process:
                    logger.info(
                        f"Task {task_id} is running in background (PID: {process.pid})"
                    )

                    # Wait for this task to complete if shutdown flag is not set
                    if not shutdown_flag.is_set():
                        process.wait()  # Wait for the process to finish
                        logger.info(
                            f"Process {process.pid} for task {task_id} has finished."
                        )

                        # Update task status based on process return code
                        if process.returncode == 0:
                            Task.objects.filter(id=task_id).update(
                                status="completed", completed_at=timezone.now()
                            )
                            logger.info(f"Task {task_id} completed successfully.")
                        else:
                            Task.objects.filter(id=task_id).update(status="failed")
                            logger.error(
                                f"Task {task_id} failed with return code {process.returncode}."
                            )
                else:
                    Task.objects.filter(id=task_id).update(status="failed")
                    logger.error(f"Failed to start task {task_id}.")

        except Exception as e:
            logger.error(f"Error in worker {worker.name}: {str(e)}")
            time.sleep(1)

    logger.info(f"Worker {worker.name} shutting down...")
    worker.status = "inactive"
    worker.save()


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

    def handle(self, *args, **options):
        num_workers = options["workers"]
        batch_size = options["batch_size"]

        shutdown_flag = multiprocessing.Event()
        processes = []

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Stopping workers...")
            shutdown_flag.set()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        logger.info(f"Starting {num_workers} workers...")

        for i in range(num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(batch_size, shutdown_flag, i),
            )
            p.start()
            processes.append(p)

        try:
            while any(p.is_alive() for p in processes):
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Stopping workers...")
            shutdown_flag.set()

        # Wait for all processes to finish
        for p in processes:
            p.join()

        logger.info("All workers have shut down. Main process exiting.")
