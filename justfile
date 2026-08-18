default:
    cargo build

# Docker
up:
    docker compose up -d
up-build:
    docker compose up --build -d
down:
    docker compose down
db-redis:
    docker compose up -d --wait db redis

# Local dev
dev: db-redis
    cargo build --workspace --bins
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
