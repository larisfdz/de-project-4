drop table if exists dds.dm_delivery_timestamps cascade;

drop table if exists dds.dm_couriers cascade;

DROP TABLE if exists dds.dm_delivery_addesses cascade;

drop table if exists dds.dm_deliveries cascade;

drop table if exists dds.fct_delivery_payments cascade;

create table if not exists dds.dm_delivery_timestamps (
	id serial4 primary key,
	ts timestamp not null,
	"year" int2 not null,
	"month" int2 not null,
	"day" int2 not null,
	"time" time not null,
	"date" date not null,
	constraint dm_delivery_timestamps_day_check check (
		(
			(day >= 1)
			and (day <= 31)
		)
	),
	constraint dm_delivery_timestamps_month_check check (
		(
			(month >= 1)
			and (month <= 12)
		)
	),
	constraint dm_delivery_timestamps_year_check check (
		(
			(year >= 2022)
			and (year < 2500)
		)
	)
);

create table if not exists dds.dm_couriers (
	id serial primary key,
	courier_id varchar not null,
	courrier_name varchar not null,
	active_from timestamp not null,
	active_to timestamp not null
);

CREATE TABLE if not exists dds.dm_delivery_addresses (
	id serial primary key,
	full_address varchar not null
);

create table if not exists dds.dm_deliveries (
	id serial4 primary key,
	order_id int4 not null,
	courier_id int4 not null,
	timestamp_id int4 not null,
	full_address_id int not null,
	delivery_key varchar not null,
	rate int,
	constraint dm_deliveries_order_id_fkey foreign key (order_id) references dds.dm_orders(id),
	constraint dm_deliveries_courier_id_fkey foreign key (courier_id) references dds.dm_couriers(id),
	constraint dm_deliveries_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_delivery_timestamps(id),
	constraint dm_deliveries_full_address_id_fkey foreign key (full_address_id) references dds.dm_delivery_addresses(id),
	constraint dm_deliveries_rate_check check (
		(
			(rate >= 1)
			and (rate <= 5)
		)
	)
);

create table if not exists dds.fct_delivery_payments (
	id serial4 primary key,
	courier_id int4 not null,
	order_id int4 not null,
	timestamp_id int4 not null,
	delivery_id int4 not null,
	"sum" numeric(14, 2) not null default 0,
	tip_sum numeric(14, 2) not null default 0,
	constraint fct_delivery_payments_order_id_fkey foreign key (order_id) references dds.dm_orders(id),
	constraint fct_delivery_payments_courier_id_fkey foreign key (courier_id) references dds.dm_couriers(id),
	constraint fct_delivery_payments_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_delivery_timestamps(id),
	constraint fct_delivery_payments_sum_check check ("sum" >= 0),
	constraint fct_delivery_payments_tip_sum_check check (tip_sum >= 0)
);