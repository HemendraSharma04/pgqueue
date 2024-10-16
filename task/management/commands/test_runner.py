import os
import time
import signal
import logging
import logging.handlers
import multiprocessing
from django.core.management.base import BaseCommand
from django.db import transaction
import subprocess
import psutil

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
        cmd = f"nohup python manage.py run_single_task {task_id} > /dev/null 2>&1 & echo $!"
        process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        pid = int(process.communicate()[0].strip())
        logger.info(f"Started task {task_id} with PID {pid}")
        return pid
    except Exception as e:
        logger.error(f"Failed to start task {task_id}: {str(e)}")
        return None


def is_process_running(pid):
    try:
        process = psutil.Process(pid)
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def worker_process(worker_id, task_queue, shutdown_flag):
    django_setup()
    from task.models import Task
    from worker.models import Worker

    worker = Worker.objects.create(name=f"Worker-{worker_id}", status="active")
    logger.info(f"Worker {worker.name} (PID: {os.getpid()}) started...")

    while not shutdown_flag.is_set():
        try:
            task_id = task_queue.get(timeout=1)
            logger.info(f"Worker {worker.name} processing task {task_id}")

            with transaction.atomic():
                task = Task.objects.select_for_update(skip_locked=True).get(id=task_id)
                task.status = "processing"
                task.worker = worker
                task.save()

            pid = execute_task(task_id)
            if pid:
                logger.info(f"Task {task_id} started with PID {pid}")
                task.pid = pid
                task.save()
            else:
                logger.error(f"Failed to start task {task_id}")
                task.status = "failed"
                task.save()

        except multiprocessing.queues.Empty:
            continue
        except Exception as e:
            logger.error(f"Error in worker {worker.name}: {str(e)}")

    logger.info(f"Worker {worker.name} shutting down...")
    worker.status = "inactive"
    worker.save()


class Command(BaseCommand):
    help = "Process tasks with multiple workers concurrently"

    def add_arguments(self, parser):
        parser.add_argument(
            "--workers", type=int, default=4, help="Number of worker processes"
        )

    def handle(self, *args, **options):
        num_workers = options["workers"]
        task_queue = multiprocessing.Queue()
        shutdown_flag = multiprocessing.Event()
        processes = []

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Stopping workers...")
            shutdown_flag.set()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        for i in range(num_workers):
            p = multiprocessing.Process(
                target=worker_process, args=(i, task_queue, shutdown_flag)
            )
            p.start()
            processes.append(p)

        try:
            while not shutdown_flag.is_set():
                django_setup()
                from task.models import Task

                with transaction.atomic():
                    pending_tasks = Task.objects.filter(
                        status="pending"
                    ).select_for_update(skip_locked=True)[:100]
                    for task in pending_tasks:
                        task_queue.put(task.id)

                    # Check and update status of processing tasks
                    processing_tasks = Task.objects.filter(status="processing")
                    for task in processing_tasks:
                        if task.pid and not is_process_running(task.pid):
                            task.status = (
                                "completed"  # Or you might want to check for failure
                            )
                            task.save()

                time.sleep(5)  # Wait before checking for new tasks again

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Stopping workers...")
            shutdown_flag.set()

        for p in processes:
            p.join()

        logger.info("All workers have shut down. Exiting.")
