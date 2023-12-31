use std::env;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;
use configmanager::ConfigManager;


use indexengine::index::Index;
use indexengine::no_index::NoIndex;

use crate::server::key_value_store::key_value_service_server::KeyValueServiceServer;

mod logging_middleware;
mod server;

#[derive(Debug, Clone)]
enum IndexEngine {
    BTree,
    LSMTree,
    NoIndex,
    HashMap,
}

impl FromStr for IndexEngine {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BTree" => Ok(IndexEngine::BTree),
            "LSMTree" => Ok(IndexEngine::LSMTree),
            "NoIndex" => Ok(IndexEngine::NoIndex),
            "HashMap" => Ok(IndexEngine::HashMap),
            _ => Err("no match"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "patrick.db")]
    storage_file_name: String,
    #[arg(long, default_value = "localhost:2181,localhost:2182,localhost:2183")]
    zookeeper_servers: String,
    #[arg(long, default_value = "[::1]:50051")]
    server_address: String,
    #[arg(long, default_value = "http://[::1]:50051")]
    server_url: String,
    #[arg(long, default_value = "/latch")]
    leader_election_path: String,
    #[arg(long, default_value = "/services")]
    service_registry_path: String,
    #[arg(long, default_value = "BTree")]
    index_engine: IndexEngine,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = Args::parse();
    let server_address = env::var("SERVER_ADDRESS").ok().unwrap_or(args.server_address);
    let server_url = env::var("SERVER_URL").ok().unwrap_or(args.server_url);
    let zookeeper_servers = env::var("ZOOKEEPER_SERVERS").ok().unwrap_or(args.zookeeper_servers);
    let leader_election_path = env::var("LEADER_ELECTION_PATH").ok().unwrap_or(args.leader_election_path);
    let service_registry_path = env::var("SERVICE_REGISTRY_PATH").ok().unwrap_or(args.service_registry_path);

    log::info!("start zookeeper config manager");
    let _config_manager = configmanager::ZooKeeperConfigManager::new(
        service_registry_path.as_str(),
        leader_election_path.as_str(),
        server_url.as_str(),
        zookeeper_servers.as_str(),
    )?;
    let is_leader = _config_manager.is_leader();
    let name = _config_manager.get_name();
    let leader_address = _config_manager.get_leader_address()?;
    let follower_addresses = _config_manager.get_follower_addresses()?;
    log::info!("is leader: {}", is_leader);
    log::info!("name: {}", name);
    log::info!("leader address: {}", leader_address);
    log::info!("follower addresses: {:?}", follower_addresses);
    log::info!("finished starting zookeeper config manager");

    log::info!("init storage engine");
    let storage_file_name = env::var("STORAGE_FILE_NAME").ok().unwrap_or(args.storage_file_name);
    let file_handler = storageengine::file_handler::FileHandlerImpl::new(&storage_file_name)?;
    let operations = storageengine::operations::DbOperationsImpl::new(Box::new(file_handler));
    let _index_engine: Box<dyn Index<Vec<u8>, Vec<u8>>> = match args.index_engine {
        IndexEngine::BTree => indexengine::new_index_engine(indexengine::IndexEngine::BTree, Box::new(operations)).expect("failed to create btree"),
        IndexEngine::LSMTree => indexengine::new_index_engine(indexengine::IndexEngine::LSM, Box::new(operations)).expect("failed to create lsm"),
        IndexEngine::NoIndex => Box::new(NoIndex::new(Box::new(operations))),
        IndexEngine::HashMap => indexengine::new_index_engine(indexengine::IndexEngine::HashMap, Box::new(operations)).expect("failed to create hashmap"),
    };
    log::info!("finished init storage engine");

    // let addr = "0.0.0.0:50051".parse()?;
    let addr = server_address.parse()?;
    let server = server::KeyValueStoreImpl::default();

    let layer = tower::ServiceBuilder::new()
        // Apply middleware from tower
        .timeout(Duration::from_secs(30))
        // Apply our own middleware
        .layer(logging_middleware::LoggingInterceptorLayer)
        .into_inner();

    log::info!("start server");

    Server::builder()
        .layer(layer)
        .add_service(KeyValueServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
