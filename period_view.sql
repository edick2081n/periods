CREATE TEMP TABLE periods_data as SELECT loader_periods.id, loader_periods.endpoint_id, mode_start, mode_duration,
 mode_start+MAKE_INTERVAL(0, 0, 0, 0, 0, mode_duration, 0) as mode_end, label
 FROM loader_periods;

CREATE VIEW result as
 SELECT periods_data.id,  periods_data.endpoint_id, mode_start, mode_duration, mode_end,
 label, operator_name, COALESCE (reason, 'нет данных') as reason, energy_data.kwh
 FROM periods_data

 LEFT JOIN loader_reasons
 ON periods_data.endpoint_id=loader_reasons.endpoint_id
 AND periods_data.mode_start=loader_reasons.event_time

 LEFT JOIN loader_operators
 ON periods_data.endpoint_id=loader_operators.endpoint_id
 AND periods_data.mode_start>=TO_TIMESTAMP(loader_operators.login_time, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')
 AND periods_data.mode_end<=TO_TIMESTAMP(loader_operators.logout_time, 'YYYY-MM-DD HH24:MI:SS.US +TZH:TZM')

 LEFT JOIN (SELECT period_id, SUM(kwh) as kwh
 FROM (SELECT periods_data.id as period_id, kwh
 FROM periods_data INNER JOIN loader_energy
 ON loader_energy.endpoint_id=periods_data.endpoint_id
 AND periods_data.mode_start<=loader_energy.event_time
 AND loader_energy.event_time<=periods_data.mode_end) periods_energy GROUP BY period_id) energy_data
 ON periods_data.id=energy_data.period_id


 ORDER BY periods_data.endpoint_id DESC, mode_start asc LIMIT 100
