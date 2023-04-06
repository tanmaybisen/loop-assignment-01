-- public.store_status definition

CREATE TABLE public.store_status (
	store_id numeric(25) NULL,
	status varchar(8) NULL,
	timestamp_utc timestamp NULL
);

-- public.store_timezone definition

CREATE TABLE public.store_timezone (
	store_id numeric(25),
	timezone_str varchar(40)
);

-- public.menu_hours definition

create table menu_hours(
	store_id numeric(25), 
	dayOfWeek numeric(1), 
	start_time_local time, 
	end_time_local time
);


SELECT t.store_id,
       jsonb_build_object(
           '2023-01-18', jsonb_agg(
               json_build_object('p1', p1, 'p2', p2) ORDER BY p2
           ) FILTER (WHERE DATE(p2) = '2023-01-18'),
           '2023-01-19', jsonb_agg(
               json_build_object('p1', p1, 'p2', p2) ORDER BY p2
           ) FILTER (WHERE DATE(p2) = '2023-01-19'),
           '2023-01-20', jsonb_agg(
               json_build_object('p1', p1, 'p2', p2) ORDER BY p2
           ) FILTER (WHERE DATE(p2) = '2023-01-20'),
           '2023-01-21', jsonb_agg(
               json_build_object('p1', p1, 'p2', p2) ORDER BY p2
           ) FILTER (WHERE DATE(p2) = '2023-01-21'),
           '2023-01-22', jsonb_agg(
               json_build_object('p1', p1, 'p2', p2) ORDER BY p2
           ) FILTER (WHERE DATE(p2) = '2023-01-22'),
           '2023-01-23', jsonb_agg(
               json_build_object('p1', p1, 'p2', p2) ORDER BY p2
           ) FILTER (WHERE DATE(p2) = '2023-01-23')
       ) as status_timestamps
FROM (
    SELECT ss.store_id,
           status AS p1,
           timestamp_utc AS p2
    FROM store_status ss
    WHERE timestamp_utc >= '2023-01-18 00:00:00' AND timestamp_utc <= '2023-01-23 23:59:59'
    WINDOW w AS (PARTITION BY ss.store_id)
) AS t
GROUP BY t.store_id
ORDER BY t.store_id;
