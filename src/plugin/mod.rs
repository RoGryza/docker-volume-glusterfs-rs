use std::convert::Infallible;
use std::error::Error;
use std::fs;
use std::path::Path;

use futures::prelude::*;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyperlocal::UnixServerExt;
use log::{error, info, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{stream_reader, AsyncReadExt};
use tokio::signal::unix::{signal, SignalKind};

use crate::gluster_cli;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Sync + Send + 'static>>;

async fn service(req: Request<Body>) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    if req.method() != Method::POST {
        *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        return Ok(response);
    }
    let path = req.uri().path().to_string();
    let body_result: Result<Body> = match path.as_str() {
        "/Plugin.Activate" => Ok(r#"{"Implements": ["VolumeDriver"]}"#.into()),
        "/VolumeDriver.Capabilities" => Ok(r#"{"Capabilities": {"Scope": "global"}}"#.into()),
        "/VolumeDriver.Create" => serde_request(create, req).await,
        "/VolumeDriver.Remove" => Err("NIY".into()),
        "/VolumeDriver.Mount" => Err("NIY".into()),
        "/VolumeDriver.Unmount" => Err("NIY".into()),
        "/VolumeDriver.Path" => Err("NIY".into()),
        "/VolumeDriver.List" => Err("NIY".into()),
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
            *response.body_mut() = "Not found".into();
            return Ok(response);
        }
    };
    *response.body_mut() = match body_result {
        Ok(body) => {
            info!("{}: OK", path);
            body
        }
        Err(e) => {
            error!("{}: {}", path, e);
            let mut msg = e.to_string();
            if msg.is_empty() {
                msg = "Unknown error".to_string();
            }
            format!(r#"{{"Err": "{}"}}"#, msg.escape_default()).into()
        }
    };
    Ok(response)
}

async fn serde_request<F, G, T, U>(f: F, req: Request<Body>) -> Result<Body>
where
    F: FnOnce(T) -> G,
    G: Future<Output = Result<U>>,
    T: DeserializeOwned,
    U: Serialize,
{
    let mut req_body = Vec::new();
    stream_reader(
        req.into_body()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    )
    .read_to_end(&mut req_body)
    .await?;
    let req_data = serde_json::from_slice(&req_body)?;
    let resp_data = f(req_data).await?;
    let resp_bytes = serde_json::to_vec(&resp_data)?;
    Ok(resp_bytes.into())
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VolumeCreateRequest {
    name: String,
    opts: Options,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Options {}

async fn create(req: VolumeCreateRequest) -> Result<()> {
    let servers = [("gluster-server", format!("/mnt/{}", req.name))];
    gluster_cli::volume::create("localhost", &req.name, &servers).await?;
    Ok(())
}

pub async fn run_server<P>(socket: P) -> Result<()>
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

    result.map_err(|e| e.into())
}
