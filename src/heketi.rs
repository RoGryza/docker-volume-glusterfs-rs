use std::fmt::{Display, Formatter, Result as FmtResult, Write};
use std::time::Duration;

use chrono::prelude::*;
use futures::prelude::*;
use hyper::client::HttpConnector;
use hyper::{Body, Request, Response, StatusCode};
use jsonwebtoken::EncodingKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::delay_for;

pub struct Client {
    secret: String,
    host: String,
    http: hyper::client::Client<HttpConnector, Body>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateVolumeRequest {
    pub size: usize,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durability: Option<Durability>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Durability {
    None,
    Replicate {
        #[serde(skip_serializing_if = "Option::is_none")]
        replica: Option<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VolumeId(String);

impl Display for VolumeId {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Volume {
    pub id: VolumeId,
    pub name: String,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    iat: i64,
    exp: i64,
    qsh: &'a str,
}

impl Client {
    pub fn new(host: String, secret: String) -> Result<Self, jsonwebtoken::errors::Error> {
        Ok(Client {
            secret,
            host,
            http: hyper::client::Client::builder().build_http(),
        })
    }

    pub async fn create_volume(&self, req: &CreateVolumeRequest) -> Result<VolumeId, hyper::Error> {
        #[derive(Deserialize)]
        struct Volume {
            id: VolumeId,
        }

        let req_body = serde_json::to_vec(req).expect("TODO ser error");
        let req = self
            .request("POST", "/volumes")
            .body(Body::from(req_body))
            .expect("TODO http error");
        let mut resp_body = Vec::new();
        self.async_operation(req)
            .await?
            .body_mut()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .read_to_end(&mut resp_body)
            .await
            .expect("TODO ASPDJASLKDJ");
        let volume: Volume = serde_json::from_slice(&resp_body).expect("KAKAAKKA");
        Ok(volume.id)
    }

    pub async fn list_volumes(&self) -> Result<Vec<VolumeId>, hyper::Error> {
        #[derive(Deserialize)]
        struct Volumes {
            volumes: Vec<VolumeId>,
        }
        let mut resp_body = Vec::new();
        self.get("/volumes")
            .await?
            .body_mut()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .read_to_end(&mut resp_body)
            .await
            .expect("TODO ASPDJASLKDJ");
        let volumes: Volumes = serde_json::from_slice(&resp_body).expect("KAKAAKKA");
        Ok(volumes.volumes)
    }

    pub async fn get_volume(&self, id: &VolumeId) -> Result<Volume, hyper::Error> {
        let mut resp_body = Vec::new();
        self.get(&format!("/volumes/{}", id))
            .await?
            .body_mut()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .read_to_end(&mut resp_body)
            .await
            .expect("TODO ERROSSSDAD");
        let volume = serde_json::from_slice(&resp_body).expect("SERDE");
        Ok(volume)
    }

    pub async fn delete_volume(&self, id: &VolumeId) -> Result<(), hyper::Error> {
        let req = self
            .request("DELETE", &format!("/volumes/{}", id))
            .body(Body::empty())
            .expect("TODO errrroroarpoasjas");
        self.async_operation(req).await?;
        Ok(())
    }

    fn request(&self, method: &str, endpoint: &str) -> hyper::http::request::Builder {
        let now = Utc::now().timestamp();
        let mut hasher = Sha256::new();
        hasher.update(method);
        hasher.update("&");
        hasher.update(endpoint);
        let qsh_bs = hasher.finalize();
        let mut qsh = String::new();
        for b in qsh_bs {
            write!(qsh, "{:02x}", b).expect("wut");
        }

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &Claims {
                iss: "admin",
                iat: now,
                exp: now + 30,
                qsh: &qsh,
            },
            &EncodingKey::from_secret(self.secret.as_ref()),
        )
        .expect("TODO err");
        Request::builder()
            .uri(format!("{}{}", self.host, endpoint))
            .method(method)
            .header("Authorization", format!("Bearer {}", token))
    }

    async fn get(&self, endpoint: &str) -> Result<Response<Body>, hyper::Error> {
        self.http
            .request(
                self.request("GET", endpoint)
                    .body(Body::empty())
                    .expect("TODO err"),
            )
            .await
    }

    async fn async_operation(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        // TODO pass in expected final response: 303 or 204
        let mut first_resp = self.http.request(req).await?;
        if first_resp.status() != StatusCode::ACCEPTED {
            let mut s = String::new();
            first_resp
                .body_mut()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .into_async_read()
                .read_to_string(&mut s)
                .await
                .unwrap();
            panic!("TODO error {}: {}", first_resp.status(), s)
        }
        let operation_endpoint = match first_resp.headers().get("Location") {
            Some(l) => l.to_str().expect("TODO error"),
            None => panic!("TODO error"),
        };

        loop {
            let resp = self.get(operation_endpoint).await?;
            match resp.status() {
                StatusCode::OK => delay_for(Duration::from_secs(1)).await,
                StatusCode::SEE_OTHER => {
                    let resource_endpoint = match resp.headers().get("Location") {
                        Some(l) => l.to_str().expect("TODO error"),
                        None => panic!("TODO error"),
                    };
                    return self.get(resource_endpoint).await;
                }
                StatusCode::NO_CONTENT => return Ok(resp),
                StatusCode::INTERNAL_SERVER_ERROR => panic!("TODO request failed"),
                _ => panic!("TODO unexpected status code"),
            }
        }
    }
}
