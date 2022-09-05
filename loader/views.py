import csv
import io
import json
import datetime

from django.db.models import F, ExpressionWrapper, DurationField, DateTimeField, Value, IntegerField
from django.db import connection
from django.http import HttpResponse
from django.shortcuts import render

from loader.models import Energy, Operators, Periods, Reasons


def load_file_energy():
    file = open('energy.csv')
    spamreader = csv.DictReader(file, delimiter=';')

    n_object = []
    for row in spamreader:
        endpoint_id = row["endpoint_id"]
        event_time = row["event_time"]
        kwh = row["kwh"]
        new_object = Energy(endpoint_id=endpoint_id, event_time=event_time, kwh=kwh)
        n_object.append(new_object)

    Energy.objects.bulk_create(n_object, ignore_conflicts=True)

    return HttpResponse(json.dumps({'status': 'ok'}))

def load_file_operators():
    file = open('operators.csv', encoding="utf-8")
    spamreader = csv.DictReader(file, delimiter=';')

    n_object = []
    for row in spamreader:
        endpoint_id = row["endpoint_id"]
        login_time = row["login_time"]
        logout_time = row["logout_time"]
        operator_name = row["operator_name"]
        new_object = Operators(endpoint_id=endpoint_id,
                               login_time=login_time,
                               logout_time=logout_time,
                               operator_name=operator_name)
        n_object.append(new_object)

    Operators.objects.bulk_create(n_object, ignore_conflicts=True)

    return HttpResponse(json.dumps({'status': 'ok'}))

def load_file_periods():
    file = open('periods.csv', encoding="utf-8")
    spamreader = csv.DictReader(file, delimiter=';')

    n_object = []
    for row in spamreader:
        endpoint_id = row["endpoint_id"]
        mode_start = row["mode_start"]
        mode_duration = row["mode_duration"]
        label = row["label"]
        new_object = Periods(endpoint_id=endpoint_id,
                               mode_start=mode_start,
                               mode_duration=mode_duration,
                               label=label)
        n_object.append(new_object)

    Periods.objects.bulk_create(n_object, ignore_conflicts=True)

    return HttpResponse(json.dumps({'status': 'ok'}))

def load_file_reasons():
    file = open('reasons.csv', encoding="utf-8")
    spamreader = csv.DictReader(file, delimiter=';')

    n_object = []
    for row in spamreader:
        endpoint_id = row["endpoint_id"]
        event_time = row["event_time"]
        reason = row["reason"]

        new_object = Reasons(endpoint_id=endpoint_id,
                               event_time=event_time,
                               reason=reason)
        n_object.append(new_object)

    Reasons.objects.bulk_create(n_object, ignore_conflicts=True)

    return HttpResponse(json.dumps({'status': 'ok'}))


# def get_result():
#     a = Periods.objects.all().annotate\
#         (mode_end=ExpressionWrapper(F('mode_start')+ExpressionWrapper(F('mode_duration')*Value(60000000, output_field=IntegerField()), output_field=DurationField()), output_field=DateTimeField()))
#     print(a.query)
#     for period in a:
#         print(period.endpoint_id, period.mode_start, period.mode_end, period.mode_duration, period.label)

def pure_sql():
    with connection.cursor() as cursor:
        cursor.execute\
            (" CREATE TEMP TABLE periods_data as SELECT loader_periods.id, loader_periods.endpoint_id, mode_start, mode_duration,"
            " mode_start+MAKE_INTERVAL(0, 0, 0, 0, 0, mode_duration, 0) as mode_end, label"
            " FROM loader_periods;"

            "CREATE VIEW result as"
            " SELECT periods_data.id,  periods_data.endpoint_id, mode_start, mode_duration, mode_end,"
            " label, operator_name, COALESCE (reason, 'нет данных') as reason, energy_data.kwh"
            " FROM periods_data"

            " LEFT JOIN loader_reasons"
            " ON periods_data.endpoint_id=loader_reasons.endpoint_id"
            " AND periods_data.mode_start=loader_reasons.event_time"

            " LEFT JOIN loader_operators"
            " ON periods_data.endpoint_id=loader_operators.endpoint_id"
            " AND periods_data.mode_start>=TO_TIMESTAMP(loader_operators.login_time, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')"
            " AND periods_data.mode_end<=TO_TIMESTAMP(loader_operators.logout_time, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')"

            " LEFT JOIN (SELECT period_id, SUM(kwh) as kwh "
            " FROM (SELECT periods_data.id as period_id, kwh "
            " FROM periods_data INNER JOIN loader_energy"
            " ON loader_energy.endpoint_id=periods_data.endpoint_id"
            " AND periods_data.mode_start<=loader_energy.event_time "
            " AND loader_energy.event_time<=periods_data.mode_end) periods_energy GROUP BY period_id) energy_data"
            " ON periods_data.id=energy_data.period_id"


            " ORDER BY periods_data.endpoint_id DESC, mode_start asc LIMIT 100")


    a=Periods.objects.raw("SELECT * FROM result;")
    for period in a:
        print(period.endpoint_id, period.mode_start, period.mode_duration,
              period.mode_end, period.label, period.reason, period.operator_name, period.kwh)


# a = Periods.objects.raw("SELECT periods_data.id,  periods_data.endpoint_id, mode_start, mode_duration, mode_end,"
#                         " label, operator_name, COALESCE (reason, 'нет данных') as reason, energy_data.kwh"
#                         " FROM (SELECT loader_periods.id, loader_periods.endpoint_id, mode_start, mode_duration,"
#                         " mode_start+MAKE_INTERVAL(0, 0, 0, 0, 0, mode_duration, 0) as mode_end, label"
#                         " FROM loader_periods) periods_data"
#                         " LEFT JOIN loader_reasons"
#                         " ON periods_data.endpoint_id=loader_reasons.endpoint_id"
#                         " AND periods_data.mode_start=loader_reasons.event_time"
#                         " LEFT JOIN loader_operators"
#                         " ON periods_data.endpoint_id=loader_operators.endpoint_id"
#                         " AND periods_data.mode_start>=TO_TIMESTAMP(loader_operators.login_time, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')"
#                         " AND periods_data.mode_end<=TO_TIMESTAMP(loader_operators.logout_time, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')"
#                         " LEFT JOIN (SELECT period_id, SUM(kwh) as kwh "
#                         " FROM (SELECT periods_data.id as period_id, kwh "
#                         " FROM periods_data INNER JOIN loader_energy"
#                         " ON loader_energy.endpoint_id=periods_data.endpoint_id"
#                         " AND periods_data.mode_start<=loader_energy.event_time "
#                         " AND loader_energy.event_time<=periods_data.mode_end) periods_energy GROUP BY period_id) energy_data"
#                         " ON periods_data.id=energy_data.period_id"
#
#                         " ORDER BY periods_data.endpoint_id DESC, mode_start asc LIMIT 1000")
