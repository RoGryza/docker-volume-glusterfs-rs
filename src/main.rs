mod gluster_cli;
mod util;
mod xml;

#[async_std::main]
async fn main() {
    env_logger::init();
    println!("{:#?}", gluster_cli::volume::info().await);
}
