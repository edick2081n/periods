from django.db import models

class Energy(models.Model):
    endpoint_id = models.IntegerField()
    event_time = models.DateTimeField()
    kwh = models.FloatField()


class Operators(models.Model):
    endpoint_id = models.IntegerField()
    login_time = models.CharField(max_length=255)
    logout_time = models.CharField(max_length=255)
    operator_name = models.CharField(max_length=255)



class Periods(models.Model):
    endpoint_id = models.IntegerField()
    mode_start = models.DateTimeField()
    mode_duration = models.IntegerField()
    label = models.CharField(max_length=255)




class Reasons(models.Model):
    endpoint_id = models.IntegerField()
    event_time = models.DateTimeField()
    reason = models.CharField(max_length=255)



