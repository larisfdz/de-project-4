drop table if exists stg.del_restaurants;

drop table if exists stg.del_couriers;

drop table if exists stg.del_deliveries;

create table if not exists stg.del_restaurants (
	id serial primary key,
	_id varchar not null unique,
	name varchar not null,
	update_ts timestamp not null
);

create table if not exists stg.del_couriers (
	id serial primary key,
	_id varchar not null unique,
	name varchar not null,
	update_ts timestamp not null
);

create table if not exists stg.del_deliveries (
	id serial primary key,
	order_id varchar not null,
	order_ts timestamp not null,
	delivery_id varchar not null unique,
	courier_id varchar not null,
	address varchar not null,
	delivery_ts timestamp not null,
	rate int not null,
	"sum" numeric(14, 2) not null,
	tip_sum numeric(14, 2)
);