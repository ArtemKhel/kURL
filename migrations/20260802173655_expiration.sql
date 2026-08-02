alter table links
    add column expiration timestamptz default null;

create index links_expiration_idx
    on links (expiration)
    where expiration is not null;


-- alter system set cron.database_name = 'kurlyk';
create extension if not exists pg_cron;

select cron.schedule(
               'delete-expired-links',
               '0 3 * * *',
               $$delete from links where expiration < now() - interval '90 days'$$
       );
