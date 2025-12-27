CREATE VIEW productivity_table AS
SELECT	cal.title AS event_title,
		fil.day::timestamp AS date,
		CASE	WHEN weekday = 0 THEN 'Monday'
			    WHEN weekday = 1 THEN 'Tuesday'
			    WHEN weekday = 2 THEN 'Wednesday'
			    WHEN weekday = 3 THEN 'Thursday'
			    WHEN weekday = 4 THEN 'Friday'
			    WHEN weekday = 5 THEN 'Saturday'
				WHEN weekday = 6 THEN 'Sunday'
				END AS day_of_week,
		cal.start_date AS event_start_date,
		cal.end_date AS event_end_date,
		cal.duration_hr AS event_duration,
		cal.lead_time_hr,
		cal.title_length,
		cal.mentions_wavelength_lightsource,
		fil.mother_folder,
		fil.file_count
FROM calendar_events AS cal
JOIN files AS fil
	ON cal.title ILIKE CONCAT('%', fil.mother_folder, '%') 
	AND fil.day >= cal.start_date::date
	AND fil.day < cal.end_date::date + 1
WHERE cal.status ILIKE 'confirmed' 