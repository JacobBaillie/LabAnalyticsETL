CREATE TABLE calendar_events AS

SELECT	d1.title,
		d1.start_ts AS start_date,
		d1.end_ts AS end_date,
		d1.created_ts AS creation_date,
		d1.updated_ts AS updated_date,
		d1.status,
		d2.duration_min/60 AS duration_hr,
		d2.lead_time_hr,
		d2.weekday,
		d2.is_weekend,
		LENGTH(d1.title) AS title_length,
		d2.mentions_wavelength_lightsource
FROM raw_events_deid_2025 AS d1
JOIN event_features_2025 AS d2 ON d1.event_pk = d2.event_pk

UNION ALL

SELECT	d3.title,
		d3.start_ts AS start_date,
		d3.end_ts AS end_date,
		d3.created_ts AS creation_date,
		d3.updated_ts AS updated_date,
		d3.status,
		d4.duration_min/60 AS duration_hr,
		d4.lead_time_hr,
		d4.weekday,
		d4.is_weekend,
		LENGTH(d3.title) AS title_length,
		d4.mentions_wavelength_lightsource
FROM raw_events_deid AS d3
JOIN event_features AS d4 ON d3.event_pk = d4.event_pk

ORDER BY start_date ASC