default:
    cargo build --workspace --bins

# Docker
up:
    docker compose up -d
up-build:
    docker compose up --build -d
down:
    docker compose down
db:
    docker compose up -d --wait db
db-redis: db
    docker compose up -d --wait redis

migrate: db
    cargo run -p migrator

# Local dev
dev: db-redis migrate default
    kitty --directory "{{justfile_directory()}}" --title "Dev" "{{justfile_directory()}}/scripts/dev-session"

# Utils
fmt:
    cargo fmt
clippy:
    cargo clippy
check: fmt clippy

sqlx-meta:
    #!/usr/bin/env bash
    docker compose ps db | grep -q "Up" && already_running=true || already_running=false

    if [ "$already_running" = false ]; then
        docker compose up -d db
    fi

    cargo sqlx prepare --workspace

    if [ "$already_running" = false ]; then
        docker compose down db
    fi

open: up
    xdg-open 'http://localhost:3000'
grafana: up
    xdg-open 'http://localhost:3001'
