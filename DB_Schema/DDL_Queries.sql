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