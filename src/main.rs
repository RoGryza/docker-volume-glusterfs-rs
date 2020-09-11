// TODO create error type
mod heketi;
mod plugin;
mod util;

use log::error;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::new().default_filter_or("docker_volume_glusterfs_rs=INFO"),
    )
    .init();

    let client = heketi::Client::new("http://localhost:8080".into(), "admin".into())
        .expect("Failed to connect to client");

    if let Err(e) = plugin::run_server("glusterfs.sock", client).await {
        error!("{}", e);
    }
}
