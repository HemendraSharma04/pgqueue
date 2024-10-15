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


class WorkerProcess:
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.shutdown_flag = multiprocessing.Event()

    def create_worker(self):
        worker = Worker.objects.create(name=f"Worker-{os.getpid()}", status="active")
        return worker

    def process_task(self, task_id):
        task = Task.objects.get(id=task_id)
        computation_time = random.randint(5, 10)  # 5 to 10 seconds
        time.sleep(computation_time)
        task.result = f"Computed for {computation_time} seconds"
        task.status = "completed"
        task.completed_at = timezone.now()
        task.counter = 1
        task.save()

    def run(self):
        worker = self.create_worker()
        print(f"Worker {worker.name} (PID: {os.getpid()}) started...")

        while not self.shutdown_flag.is_set():
            try:
                with transaction.atomic():
                    tasks = (
                        Task.objects.filter(status="pending")
                        .select_for_update(skip_locked=True)
                        .order_by("created_at")[: self.batch_size]
                    )
                    task_ids = list(tasks.values_list("id", flat=True))

                    if not task_ids:
                        time.sleep(0.1)  # Short sleep if no tasks found
                        continue

                    Task.objects.filter(id__in=task_ids).update(
                        status="processing", worker=worker
                    )

                # Process tasks
                for task_id in task_ids:
                    if self.shutdown_flag.is_set():
                        break
                    self.process_task(task_id)

            except Exception as e:
                print(f"Error in worker {worker.name}: {str(e)}")
                time.sleep(1)  # Wait a bit before trying again

        print(f"Worker {worker.name} (PID: {os.getpid()}) shutting down...")
        worker.status = "inactive"
        worker.save()


class Command(BaseCommand):
    help = "Manage background worker processes"

    def add_arguments(self, parser):
        parser.add_argument("--workers", type=int, default=5)
        parser.add_argument("--batch-size", type=int, default=10)

    def handle(self, *args, **options):
        num_workers = options["workers"]
        batch_size = options["batch_size"]

        self.worker_processes = []
        self.shutdown_flag = multiprocessing.Event()

        def start_workers(num):
            new_workers = []
            for _ in range(num):
                worker = WorkerProcess(batch_size)
                p = multiprocessing.Process(target=worker.run)
                p.start()
                new_workers.append((p, worker))
            return new_workers

        def stop_workers(workers_to_stop):
            for _, worker in workers_to_stop:
                worker.shutdown_flag.set()
            for p, _ in workers_to_stop:
                p.join(
                    timeout=60
                )  # Give each worker up to 60 seconds to shut down gracefully
            return [p for p, _ in workers_to_stop if p.is_alive()]

        def restart_workers():
            self.stdout.write("Restarting workers...")
            new_workers = start_workers(num_workers)
            old_workers = self.worker_processes
            self.worker_processes = new_workers

            # Stop old workers
            remaining = stop_workers(old_workers)
            if remaining:
                self.stdout.write(
                    f"Warning: {len(remaining)} workers did not shut down gracefully and were forced to terminate."
                )

        def sigterm_handler(signum, frame):
            self.stdout.write("Received SIGTERM. Gracefully shutting down workers...")
            self.shutdown_flag.set()

        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGHUP, lambda signum, frame: restart_workers())

        self.worker_processes = start_workers(num_workers)

        while not self.shutdown_flag.is_set():
            time.sleep(1)

        remaining = stop_workers(self.worker_processes)
        if remaining:
            self.stdout.write(
                f"Warning: {len(remaining)} workers did not shut down gracefully and were forced to terminate."
            )

        self.stdout.write(self.style.SUCCESS("All workers have been shut down."))


# if __name__ == "__main__":
#     command = Command()
#     command.run_from_argv(["manage.py", "worker_manager"])
