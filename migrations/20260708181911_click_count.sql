-- Add migration script here

alter table links
    add column click_count integer default 0;