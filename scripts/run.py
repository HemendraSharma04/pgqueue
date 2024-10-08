import subprocess
import time

# Define the command to run
command = [
    "poetry",
    "run",
    "python",
    "manage.py",
    "process_task",
    "--runtime",
    "60",
    "--batch-size",
    "1000",
]

# Number of instances to run simultaneously
num_instances = 20

# List to hold subprocesses
processes = []

try:
    # Start the specified number of processes
    for _ in range(num_instances):
        # Start the subprocess
        proc = subprocess.Popen(command)
        processes.append(proc)
        time.sleep(1)  # Optional: sleep to stagger the start time

    # Wait for all processes to complete
    for proc in processes:
        proc.wait()

except KeyboardInterrupt:
    print("Terminating processes...")
    for proc in processes:
        proc.terminate()  # Terminate the processes if needed
finally:
    print("All processes have completed.")
