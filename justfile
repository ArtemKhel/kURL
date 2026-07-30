default:
    cargo build

up:
    docker compose up -d
up-build:
    docker compose up --build -d
down:
    docker compose down

fmt:
    cargo fmt
clippy:
    cargo clippy
check: fmt clippy

open: up
    xdg-open 'http://localhost:3000'
grafana: up
    xdg-open 'http://localhost:3001'
