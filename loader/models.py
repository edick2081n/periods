from django.db import models
from view_table.models import ViewTable
from django.db.models import F, ExpressionWrapper, DurationField, DateTimeField, Value, IntegerField, Prefetch, \
    OuterRef, Subquery, Func, Sum, FloatField
from django.db.models.functions import Cast, TruncTime, TruncMinute, ExtractMinute, Coalesce


class Endpoint(models.Model):
    pass


class Energy(models.Model):
   # endpoint_id = models.IntegerField()
    endpoint = models.ForeignKey(Endpoint, on_delete=models.CASCADE)
    event_time = models.DateTimeField()
    kwh = models.FloatField()


class Operators(models.Model):
    #endpoint_id = models.IntegerField()
    endpoint = models.ForeignKey(Endpoint, on_delete=models.CASCADE)
    login_time = models.CharField(max_length=255)
    logout_time = models.CharField(max_length=255)
    operator_name = models.CharField(max_length=255)



class Periods(models.Model):
    #endpoint_id = models.IntegerField()
    endpoint = models.ForeignKey(Endpoint, on_delete=models.CASCADE)
    mode_start = models.DateTimeField()
    mode_duration = models.IntegerField()
    label = models.CharField(max_length=255)


class Reasons(models.Model):
    #endpoint_id = models.IntegerField()
    endpoint = models.ForeignKey(Endpoint, on_delete=models.CASCADE)
    event_time = models.DateTimeField()
    reason = models.CharField(max_length=255)

class MakeIntervalSeconds(Func):
    function = 'MAKE_INTERVAL'
    template = '%(function)s(0, 0, 0, 0, 0, %(expressions)s, 0)'


class ToTimeStamp(Func):
    function = 'TO_TIMESTAMP'
    template = "%(function)s(%(expressions)s, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')"

class PeriodResultView(ViewTable):
    #period = models.OneToOneField(Periods, primary_key=True, on_delete=models.CASCADE)
    reason = models.CharField(max_length=255)
    operator_name = models.CharField(max_length=255)
    mode_start = models.DateTimeField()
    mode_end = models.DateTimeField()
    endpoint_id = models.IntegerField()
    mode_duration = models.IntegerField()
    kwh = models.FloatField()

    @classmethod
    def get_query(cls):
        x = Reasons.objects.filter(event_time=OuterRef("mode_start"), endpoint_id=OuterRef('endpoint_id'))
        y = Operators.objects.annotate(dt_login_time=ToTimeStamp(F('login_time')),
                                       dt_logout_time=ToTimeStamp(F('logout_time'))).filter(
            dt_login_time__lte=OuterRef("mode_start"),
            dt_logout_time__gte=ExpressionWrapper(
                OuterRef('mode_start') + MakeIntervalSeconds(OuterRef('mode_duration')), output_field=DateTimeField()),
            endpoint_id=OuterRef('endpoint_id'))
        t = Energy.objects.filter(endpoint_id=OuterRef('endpoint_id'),
                                  event_time__gte=OuterRef('mode_start'),
                                  event_time__lte=ExpressionWrapper(
                                      OuterRef('mode_start') + MakeIntervalSeconds(OuterRef('mode_duration')),
                                      output_field=DateTimeField())) \
            .values('endpoint_id') \
            .annotate(total_kwh=Sum('kwh')).values('total_kwh')

        a = Periods.objects.annotate(reason=Coalesce(Subquery(x.values('reason')[:1]), Value("'нет данных'"))) \
            .annotate(mode_end=ExpressionWrapper(F('mode_start') + MakeIntervalSeconds(F('mode_duration')),
                                                 output_field=DateTimeField())) \
            .annotate(operator_name=Subquery(y.values('operator_name')[:1])) \
            .annotate(kwh=Subquery(t[:1], output_field=FloatField()))
            # .values('reason', 'mode_end', 'operator_name', 'kwh', 'mode_start', 'mode_duration', 'endpoint_id')
        return str(a.query)