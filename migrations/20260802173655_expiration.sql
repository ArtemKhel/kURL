alter table links
    add column if not exists expiration timestamptz default null;

create index if not exists links_expiration_idx
    on links (expiration)
    where expiration is not null;


create extension if not exists pg_cron;

select cron.schedule(
               'delete-expired-links',
               '0 3 * * *',
               $$delete from links where expiration < now() - interval '90 days'$$
       );
