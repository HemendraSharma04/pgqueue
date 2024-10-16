from django.core.management.base import BaseCommand
from task.models import Task
import time
import logging
import django
import os

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
# formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s")
# syslog_handler.setFormatter(formatter)
# logger.addHandler(syslog_handler)


def django_setup():
    import django

    django.setup()



def process_task(task_id):
    """Process a single task."""

    # os.setsid()
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
            time.sleep(1)

        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = os.getpid()
        task.save()
        print(f"Task {task_id} completed.")
    except Exception as e:
        logger.error(f"Error processing task {task_id}: {str(e)}")


class Command(BaseCommand):
    help = "Process a single task"

    def add_arguments(self, parser):
        parser.add_argument("task_id", type=str)

    def handle(self, *args, **options):
        task_id = options["task_id"]
        process_task(task_id)
