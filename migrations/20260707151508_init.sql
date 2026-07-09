-- Add migration script here

create table links
(
    id         serial primary key,
    short_code text unique not null,
    target     text        not null
)