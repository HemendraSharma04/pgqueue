import time
import signal
from datetime import datetime
from django.core.management.base import BaseCommand
from multiprocessing import Process, Queue, current_process

# Global flag to signal shutdown
shutdown_flag = False


def write_to_file(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("service_log.txt", "a") as f:
        f.write(f"{timestamp} - {message}\n")


def signal_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    write_to_file(f"Process {current_process().name} received shutdown signal.")


def long_running_task(task_id):
    """Simulate a long-running task that periodically checks for shutdown signal."""
    write_to_file(f"Task {task_id} started by {current_process().name}")
    start_time = time.time()
    while time.time() - start_time < 15:  # Run for 15 seconds
        if shutdown_flag:
            write_to_file(
                f"Task {task_id} interrupted after {time.time() - start_time:.2f} seconds"
            )
            return
        time.sleep(5)  # Check for shutdown every 5 seconds
    write_to_file(
        f"Task {task_id} completed by {current_process().name} and id {current_process().id}"
    )


def worker(task_queue):
    """Worker function to process tasks."""
    signal.signal(signal.SIGTERM, signal_handler)
    while not task_queue.empty() and not shutdown_flag:
        try:
            task_id = task_queue.get(timeout=1)
            long_running_task(task_id)
        except Queue.Empty:
            pass
    write_to_file(f"Worker {current_process().name} shutting down.")


class Command(BaseCommand):
    help = "Run multiple processes, each executing long tasks once."

    def handle(self, *args, **kwargs):
        num_workers = 1  # Number of worker processes
        num_tasks = 5  # Total number of tasks to run

        write_to_file("Starting service runner")

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
            for p in processes:
                p.join(timeout=200)  # Give each process 30 seconds to shut down
                if p.is_alive():
                    write_to_file(
                        f"Process {p.name} did not shut down gracefully. Terminating."
                    )
                    p.terminate()

        write_to_file("All tasks have been completed or service has been shut down.")
