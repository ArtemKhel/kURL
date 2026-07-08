-- Add migration script here

alter table links
    add constraint short_code_length check (char_length(short_code) > 0 and char_length(short_code) <= 40);