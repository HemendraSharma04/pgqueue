import random
from django.db import models
from django.utils import timezone


class Data(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    data = models.JSONField()
    flag = models.BooleanField(default=False)
    counter = models.IntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name
