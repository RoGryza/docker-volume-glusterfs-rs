// TODO create error type
mod gluster_cli;
mod plugin;
mod util;
mod xml;

use log::error;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::new().default_filter_or("docker_volume_glusterfs_rs=INFO"),
    )
    .init();
    if let Err(e) = plugin::run_server("glusterfs.sock").await {
        error!("{}", e);
    }
}
