from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from task.models import Task
import time
import random
import signal
import sys
import redis
import logging
from logging.handlers import SysLogHandler

# Local Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
syslog_handler = SysLogHandler(address="/dev/log")
formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s")
syslog_handler.setFormatter(formatter)
logger.addHandler(syslog_handler)


class Command(BaseCommand):
    help = "Process tasks with workers that fetch their own tasks"

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=5)

    def __init__(self):
        super().__init__()
        self.running = True
        self.current_restart_id = None

    def check_for_restart(self):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        restart_id = r.get("restart_id")
        if restart_id is not None:
            restart_id = restart_id.decode("utf-8")
            if self.current_restart_id is None:
                self.current_restart_id = restart_id
            elif restart_id != self.current_restart_id:
                logger.warning("restart id changed!!!!!!!!!!!!!!!!!!")
                logger.warning(
                    f"Restart ID changed from {self.current_restart_id} to {restart_id}. Initiating graceful restart..."
                )
                return True
        return False

    def process_task(self, task):
        logger.info(f"Running task with id {task.id}")
        computation_time = random.randint(5, 10)  # 5 to 10 seconds
        time.sleep(computation_time)
        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()

    def signal_handler(self, signum, frame):
        logger.info("Received termination signal. Initiating graceful shutdown...")
        self.running = False

    def handle(self, *args, **options):
        batch_size = options["batch-size"]
        logger.info(f"Starting task processing...")

        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        while self.running:
            if self.check_for_restart():
                logger.info("Restarting due to restart_id change...")
                sys.exit(0)  # Exit with success code to allow systemd to restart

            try:
                with transaction.atomic():
                    tasks = (
                        Task.objects.filter(status="pending")
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[:batch_size]
                    )
                    task_ids = list(tasks.values_list("id", flat=True))

                    if not task_ids:
                        time.sleep(0.1)
                        continue

                    Task.objects.filter(id__in=task_ids).update(status="processing")

                for task_id in task_ids:
                    if not self.running:
                        break
                    task = Task.objects.get(id=task_id)
                    self.process_task(task)

            except Exception as e:
                logger.error(f"Error in processing tasks: {str(e)}")
                time.sleep(1)

        logger.info("Graceful shutdown complete. Exiting.")
