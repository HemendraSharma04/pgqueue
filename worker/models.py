import socket
from django.db import models
from django.utils import timezone


class Worker(models.Model):
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, default="active")
    created_at = models.DateTimeField(auto_now_add=True)
    ip = models.GenericIPAddressField(blank=True, null=True)

    def save(self, *args, **kwargs):
        if not self.ip:
            # Fetch the IP address of the instance
            try:
                hostname = socket.gethostname()
                self.ip = socket.gethostbyname(hostname)
            except Exception as e:
                self.ip = "Unknown"  # Fallback in case of any errors
        super(Worker, self).save(*args, **kwargs)

    def __str__(self):
        return f"Worker {self.id}"

    class Meta:
        db_table = "workers"
