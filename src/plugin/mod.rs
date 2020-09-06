use std::convert::Infallible;
use std::fs;
use std::path::Path;

use futures::prelude::*;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyperlocal::UnixServerExt;
use log::warn;
use tokio::signal::unix::{signal, SignalKind};

async fn service(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut response = Response::new(Body::empty());
    if req.method() != Method::POST {
        *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        return Ok(response);
    }
    let body = match req.uri().path() {
        "/Plugin.Activate" => r#"{"Implements": ["VolumeDriver"]}"#.into(),
        "/VolumeDriver.Capabilities" => r#"{"Capabilities": {"Scope": "global"}}"#.into(),
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
            "Not found".into()
        }
    };
    *response.body_mut() = body;
    Ok(response)
}

pub async fn run_server<P>(socket: P) -> Result<(), hyper::Error>
where
    P: AsRef<Path>,
{
    let make_svc = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(service)) });

    let socket_path = socket.as_ref();
    let server = Server::bind_unix(socket_path)
        .expect("TODO")
        .serve(make_svc);
    let result = match signal(SignalKind::interrupt()) {
        Ok(mut term) => server.with_graceful_shutdown(term.next().map(|_| ())).await,
        Err(e) => {
            warn!("Failed to install TERM handler: {}", e);
            server.await
        }
    };

    if socket_path.exists() {
        if let Err(e) = fs::remove_file(socket_path) {
            warn!(
                "Failed to remove socket file at {}: {}",
                socket.as_ref().display(),
                e
            );
        }
    }

    result
}
