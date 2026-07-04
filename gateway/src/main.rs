use axum::response::Html;
use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    let app = Router::new().route("/", get(hello));
    let listener = TcpListener::bind("localhost:3000").await?;
    axum::serve(listener, app).await
}

async fn hello() -> Html<&'static str> {
    Html("<h1>Hello, World!</h1>")
}