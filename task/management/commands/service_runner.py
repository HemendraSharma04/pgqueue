from django.core.management.base import BaseCommand
from multiprocessing import Process, Queue, current_process
import time
import logging

# Get a logger instance for this module
logger = logging.getLogger(__name__)


def long_running_task(task_id):
    """Simulate a 10-minute task."""
    logger.info(f"Task {task_id} started by {current_process().name}")
    time.sleep(60)  # Simulate 10 minutes
    logger.info(f"Task {task_id} completed by {current_process().name}")


def worker(task_queue):
    """Worker function to process tasks."""
    while not task_queue.empty():
        task_id = task_queue.get()
        long_running_task(task_id)


class Command(BaseCommand):
    help = "Run multiple processes, each executing long tasks."

    def handle(self, *args, **kwargs):
        num_workers = 5  # Number of worker processes
        num_tasks = 5  # Total number of tasks to run

        # Create a queue to hold the tasks
        task_queue = Queue()
        for task_id in range(num_tasks):
            task_queue.put(task_id)

        # Start the worker processes
        processes = []
        logger.info("Starting worker processes...")
        for i in range(num_workers):
            p = Process(target=worker, args=(task_queue,))
            p.start()
            processes.append(p)

        # Wait for all processes to complete
        for p in processes:
            p.join()

        logger.info("All tasks have been completed.")
