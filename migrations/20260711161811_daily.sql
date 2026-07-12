-- Add migration script here

alter table links
    add column if not exists last_clicked_at timestamptz;

alter table links
    add column if not exists click_count bigint not null default 0;

create table if not exists analytics_global
(
    id           smallint primary key default 1,
    total_clicks bigint not null      default 0,
    constraint analytics_global_singleton check (id = 1)
);

insert into analytics_global (id, total_clicks)
values (1, 0)
on conflict (id) do nothing;


create table if not exists link_daily_clicks
(
    short_code text   not null references links (short_code) on delete cascade,
    day        date   not null,
    clicks     bigint not null default 0,
    primary key (short_code, day)
);

create table if not exists global_daily_clicks
(
    day    date primary key,
    clicks bigint not null default 0
);

create index if not exists link_daily_clicks_day_idx on link_daily_clicks (day);