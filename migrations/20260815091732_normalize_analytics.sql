alter table analytics_global
    add constraint analytics_global_total_clicks_non_negative check (total_clicks >= 0);

alter table links
    add constraint links_click_count_non_negative check (click_count >= 0);

alter table global_daily_clicks
    add constraint global_daily_clicks_clicks_non_negative check (clicks >= 0);

alter table link_daily_clicks
    add constraint link_daily_clicks_clicks_non_negative check (clicks >= 0);
