import time
import signal
import os
from datetime import datetime
from django.core.management.base import BaseCommand
from multiprocessing import Process, Queue, current_process, Empty

# Global flag to signal shutdown
shutdown_flag = False


def write_to_file(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("service_log.txt", "a") as f:
        f.write(f"{timestamp} - {message}\n")


def signal_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    write_to_file(
        f"Process {current_process().name} (PID: {os.getpid()}) received shutdown signal."
    )


def long_running_task(task_id):
    """Simulate a long-running task that prints PID, name, and timestamp."""
    pid = os.getpid()
    process_name = current_process().name
    start_time = time.time()

    while time.time() - start_time < 150:  # Run for 150 seconds
        time.sleep(5)  # Print every 5 seconds
        write_to_file(f"Task {task_id} in {process_name} (PID: {pid})")

    write_to_file(f"Task {task_id} completed by {process_name} (PID: {pid})")


def worker(task_queue):
    """Worker function to process tasks with graceful shutdown handling."""
    signal.signal(signal.SIGTERM, signal_handler)
    current_task = None
    task_start_time = None

    while not shutdown_flag:
        try:
            if current_task is None:
                current_task = task_queue.get(timeout=1)
                task_start_time = time.time()
                write_to_file(
                    f"Starting task {current_task} in {current_process().name} (PID: {os.getpid()})"
                )

            if (
                time.time() - task_start_time < 150
            ):  # Allow task to run for up to 150 seconds
                long_running_task(current_task)
                current_task = None
            else:
                write_to_file(
                    f"Task {current_task} in {current_process().name} (PID: {os.getpid()}) timed out"
                )
                current_task = None

        except Empty:
            if task_queue.empty() and current_task is None:
                break

    # Graceful shutdown: allow current task to complete if within time limit
    if current_task is not None:
        remaining_time = max(0, 150 - (time.time() - task_start_time))
        write_to_file(
            f"Allowing task {current_task} to complete (up to {remaining_time:.2f} seconds)"
        )
        shutdown_start = time.time()
        while time.time() - shutdown_start < remaining_time:
            if time.time() - task_start_time >= 150:
                break
            time.sleep(1)

    write_to_file(
        f"Worker {current_process().name} (PID: {os.getpid()}) shutting down."
    )


class Command(BaseCommand):
    help = "Run multiple processes, each executing long tasks with process-level graceful shutdown."

    def handle(self, *args, **kwargs):
        num_workers = 1  # Number of worker processes
        num_tasks = 5  # Total number of tasks to run

        write_to_file(f"Starting service runner (PID: {os.getpid()})")

        # Create a queue to hold the tasks
        task_queue = Queue()
        for task_id in range(num_tasks):
            task_queue.put(task_id)

        # Start the worker processes
        processes = []
        for i in range(num_workers):
            p = Process(target=worker, args=(task_queue,))
            p.start()
            processes.append(p)

        # Wait for all processes to complete
        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            write_to_file("Received interrupt. Shutting down gracefully...")
            global shutdown_flag
            shutdown_flag = True

            # Allow up to 150 seconds for graceful shutdown
            shutdown_start = time.time()
            while time.time() - shutdown_start < 150:
                if all(not p.is_alive() for p in processes):
                    break
                time.sleep(1)

            for p in processes:
                if p.is_alive():
                    write_to_file(
                        f"Process {p.name} (PID: {p.pid}) did not shut down gracefully. Terminating."
                    )
                    p.terminate()

        write_to_file("All tasks have been completed or service has been shut down.")
