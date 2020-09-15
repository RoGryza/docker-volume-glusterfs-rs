mod state;

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::prelude::*;
use heketi::VolumeId;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyperlocal::UnixServerExt;
use log::{error, info, warn};
use serde::de::{DeserializeOwned, Deserializer};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use tokio::io::{stream_reader, AsyncReadExt};
use tokio::signal::unix::{signal, SignalKind};

use crate::heketi;
use state::State;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Sync + Send + 'static>>;

async fn service(
    state: Arc<State>,
    client: Arc<heketi::Client>,
    req: Request<Body>,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    if req.method() != Method::POST {
        *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        return Ok(response);
    }
    let path = req.uri().path().to_string();
    let body_result: Result<Body> = match path.as_str() {
        "/Plugin.Activate" => Ok(r#"{"Implements": ["VolumeDriver"]}"#.into()),
        "/VolumeDriver.Capabilities" => Ok(r#"{"Capabilities": {"Scope": "global"}}"#.into()),
        "/VolumeDriver.Create" => serde_request(|r| create(&state, &client, r), req).await,
        "/VolumeDriver.Remove" => serde_request(|r| remove(&state, &client, r), req).await,
        "/VolumeDriver.Mount" => Err("NIY".into()),
        "/VolumeDriver.Unmount" => Err("NIY".into()),
        "/VolumeDriver.Path" => Err("NIY".into()),
        "/VolumeDriver.Get" => serde_request(|r| get(&state, r), req).await,
        "/VolumeDriver.List" => serde_request(|r| list(&state, r), req).await,
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
            *response.body_mut() = "Not found".into();
            return Ok(response);
        }
    };
    let (status, body) = match body_result {
        Ok(body) => {
            info!("{}: OK", path);
            (StatusCode::OK, body)
        }
        Err(e) => {
            error!("{}: {}", path, e);
            let mut msg = e.to_string();
            if msg.is_empty() {
                msg = "Unknown error".to_string();
            }
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(r#"{{"Err": "{}"}}"#, msg.escape_default()).into(),
            )
        }
    };
    *response.status_mut() = status;
    *response.body_mut() = body;
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

struct Empty;

impl Serialize for Empty {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_struct("Empty", 0)?.end()
    }
}

impl<'de> Deserialize<'de> for Empty {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct _Empty {}
        _Empty::deserialize(deserializer).map(|_| Empty)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Volume {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mountpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<()>,
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

async fn create(state: &State, client: &heketi::Client, req: VolumeCreateRequest) -> Result<Empty> {
    let volume_req = heketi::CreateVolumeRequest {
        size: 1,
        name: req.name,
        durability: None,
    };
    let id = client.create_volume(&volume_req).await?;
    state.set_id(volume_req.name, id).await?;
    Ok(Empty)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VolumeRemoveRequest {
    name: String,
}

async fn remove(state: &State, client: &heketi::Client, req: VolumeRemoveRequest) -> Result<Empty> {
    let id = state
        .pop_id(&req.name)
        .await?
        .ok_or_else(|| format!("Volume {} not found", req.name))?;
    client.delete_volume(&id).await?;
    Ok(Empty)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct VolumeGetRequest {
    name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct VolumeGetResponse {
    volume: Volume,
}

async fn get(state: &State, req: VolumeGetRequest) -> Result<VolumeGetResponse> {
    let _ = state
        .get_id(&req.name)
        .await?
        .ok_or_else(|| format!("Volume {} not found", req.name))?;
    Ok(VolumeGetResponse {
        volume: Volume {
            name: req.name,
            mountpoint: None,
            status: None,
        },
    })
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct VolumeListResponse {
    volumes: Vec<Volume>,
}

async fn list(state: &State, _req: Empty) -> Result<VolumeListResponse> {
    let volume_ids = state.list().await?;
    let volumes = volume_ids
        .into_iter()
        .map(|(name, st)| Volume {
            name: name,
            mountpoint: st.mount.map(|s| s.path.display().to_string()),
            status: None,
        })
        .collect();
    Ok(VolumeListResponse { volumes })
}

pub async fn run_server<P>(socket: P, db_path: &str, client: heketi::Client) -> Result<()>
where
    P: AsRef<Path>,
{
    let client = Arc::new(client);
    let state = Arc::new(State::read(db_path, client.clone()).await?);
    let make_svc = make_service_fn(move |_conn| {
        let service_state = state.clone();
        let service_client = client.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |r| {
                service(service_state.clone(), service_client.clone(), r)
            }))
        }
    });

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
