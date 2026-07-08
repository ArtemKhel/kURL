use std::time::{SystemTime, UNIX_EPOCH};

use askama::Template;
use axum::response::Html;

#[derive(Template)]
#[template(path = "hello.html")]
struct HelloTemplate<'a> {
    name: &'a str,
    time: u64,
}

pub(crate) async fn hello() -> Html<String> {
    let template = HelloTemplate {
        name: "world",
        time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
    };
    Html(template.render().unwrap())
}
