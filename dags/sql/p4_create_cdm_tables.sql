drop table if exists cdm.dm_courier_ledger;

create table if not exists cdm.dm_courier_ledger (
    id serial primary key,
    courier_id int not null,
    courrier_name varchar not null,
    settlement_year smallint not null,
    settlement_month smallint not null,
    orders_count int not null default 0,
    orders_total_sum numeric(14, 2) not null default 0,
    rate_avg numeric(14, 2),
    order_processing_fee numeric(14, 2) not null default 0,
    courier_order_sum numeric(14, 2) not null default 0,
    courier_tips_sum numeric(14, 2) not null default 0,
    courrier_reward_sum numeric(14, 2) not null default 0,
    constraint dm_courier_ledger_order_processing_fee_check check ((order_processing_fee >= (0) :: numeric)),
    constraint dm_courier_ledger_orders_count_check check ((orders_count >= (0) :: numeric)),
    constraint dm_courier_ledger_orders_total_sum_check check ((orders_total_sum >= (0) :: numeric)),
    constraint dm_courier_ledger_courier_order_sum_check check ((courier_order_sum >= 0)),
    constraint dm_courier_ledger_courier_tips_sum_check check ((courier_tips_sum >= (0) :: numeric)),
    constraint dm_courier_ledger_courrier_reward_sum_check check ((courrier_reward_sum >= (0) :: numeric)),
    constraint dm_courier_ledger_unique_check unique (courier_id, settlement_year, settlement_month)
);