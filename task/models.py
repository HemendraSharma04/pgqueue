from django.db import models
from django.utils import timezone
from worker.models import Worker
from task_queue.models import TaskQueue
# from task_detail.models import TaskDetail
import uuid

from task.constants import STATUS_CHOICES


class Task(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    # task_detail = models.ForeignKey(
    #     TaskDetail, on_delete=models.CASCADE, related_name="tasks"
    # )
    queue = models.ForeignKey(TaskQueue, on_delete=models.CASCADE, related_name="tasks")
    worker = models.ForeignKey(
        Worker, on_delete=models.SET_NULL, null=True, blank=True, related_name="tasks"
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    created_at = models.DateTimeField(auto_now_add=True)
    extra_params = models.JSONField(default=dict)
    retry_count = models.IntegerField(default=0)
    picked_at = models.DateTimeField(null=True, blank=True)
    last_picked_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    visibility_timeout = models.IntegerField(default=30)
    counter = models.IntegerField(default=0)

    def __str__(self):
        return f"Task {self.id} - {self.status}"

    class Meta:
        db_table = "tasks"
