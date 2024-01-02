use std::env;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use log::info;
use prost::Message;
use rand::Rng;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tonic::transport::Server;

use configmanager::AddressManager;
use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_client::KeyValueServiceClient;
use key_value_store::key_value_service_server::KeyValueService;

use crate::error::ServerError;
use crate::key_value_store::key_value_service_server::KeyValueServiceServer;

mod logging_middleware;
mod error;

pub mod key_value_store {
    tonic::include_proto!("server");
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "localhost:2181,localhost:2182,localhost:2183")]
    zookeeper_servers: String,
    #[arg(long, default_value = "[::1]:50051")]
    server_address: String,
    #[arg(long, default_value = "/services")]
    service_registry_paths: String,
}

pub struct KeyValueServiceRouter {
    address_managers: Arc<Mutex<Vec<Box<dyn AddressManager>>>>,
}

impl KeyValueServiceRouter {
    pub fn new(address_managers: Vec<Box<dyn AddressManager>>) -> Self {
        Self {
            address_managers: Arc::new(Mutex::new(address_managers))
        }
    }
}

#[tonic::async_trait]
impl KeyValueService for KeyValueServiceRouter {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let key_val = request.clone().key
            .ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let hash = calculate_hash(&key_bytes);
        let address = get_random_address(&self.address_managers, hash).await?;

        let mut client = KeyValueServiceClient::connect(address).await
            .map_err(|_| Status::internal("Could not connect to address"))?;
        client.get(request).await
    }

    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        let request = request.into_inner();
        let key_val = request.clone().key_value
            .ok_or_else(|| ServerError::InvalidArgument("key_value must be set".to_string()))?.key
            .ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let hash = calculate_hash(&key_bytes);
        let address = get_partitioned_leader_address(&self.address_managers, hash).await?;

        let mut client = KeyValueServiceClient::connect(address).await
            .map_err(|_| Status::internal("Could not connect to address"))?;
        client.create(request).await
    }

    async fn update(&self, request: Request<UpdateRequest>) -> Result<Response<UpdateResponse>, Status> {
        let request = request.into_inner();
        let key_val = request.clone().key_value
            .ok_or_else(|| ServerError::InvalidArgument("key_value must be set".to_string()))?.key
            .ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let hash = calculate_hash(&key_bytes);
        let address = get_partitioned_leader_address(&self.address_managers, hash).await?;

        let mut client = KeyValueServiceClient::connect(address).await
            .map_err(|_| Status::internal("Could not connect to address"))?;
        client.update(request).await
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let key_val = request.clone().key
            .ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let hash = calculate_hash(&key_bytes);
        let address = get_partitioned_leader_address(&self.address_managers, hash).await?;

        let mut client = KeyValueServiceClient::connect(address).await
            .map_err(|_| Status::internal("Could not connect to address"))?;
        client.delete(request).await
    }
}

async fn get_random_address(address_managers: &Arc<Mutex<Vec<Box<dyn AddressManager>>>>, hash: usize) -> Result<String, Status> {
    let address_managers = address_managers.lock().await;

    let address_manager_partition = hash % address_managers.len();

    let addresses = address_managers[address_manager_partition].get_all_addresses()
        .map_err(|_| Status::internal("Could not get addresses"))?;
    if addresses.is_empty() {
        return Err(Status::internal("No address managers"));
    }

    let random_address_index = rand::thread_rng().gen_range(0..addresses.len());

    Ok(addresses[random_address_index].clone())
}

async fn get_partitioned_leader_address(address_managers: &Arc<Mutex<Vec<Box<dyn AddressManager>>>>, hash: usize) -> Result<String, Status> {
    let address_managers = address_managers.lock().await;
    let address_manager_partition = hash % address_managers.len();

    let addresses = address_managers[address_manager_partition].get_leader_address()
        .map_err(|_| Status::internal("Could not get addresses"))?;

    Ok(addresses)
}

fn calculate_hash(key: &Vec<u8>) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = Args::parse();
    let server_address = env::var("SERVER_ADDRESS").ok().unwrap_or(args.server_address);
    let service_registry_paths = env::var("SERVICE_REGISTRY_PATHS").ok().unwrap_or(args.service_registry_paths);
    let zookeeper_servers = env::var("ZOOKEEPER_SERVERS").ok().unwrap_or(args.zookeeper_servers);

    info!("start address managers");
    let mut address_managers: Vec<Box<dyn AddressManager>> = Vec::new();
    for service_registry_path in service_registry_paths.split(',') {
        let address_manager = configmanager::ZooKeeperAddressManager::new(
            service_registry_path,
            zookeeper_servers.as_str(),
        ).expect("Could not create address manager");
        address_managers.push(Box::new(address_manager));
    }
    info!("finished init address managers");

    let addr = server_address.parse()?;
    let server = KeyValueServiceRouter::new(address_managers);

    let layer = tower::ServiceBuilder::new()
        // Apply middleware from tower
        .timeout(Duration::from_secs(30))
        // Apply our own middleware
        .layer(logging_middleware::LoggingInterceptorLayer)
        .into_inner();

    info!("start server");

    Server::builder()
        .layer(layer)
        .add_service(KeyValueServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    #[cfg(test)]
    use mockall::*;



    use super::*;

    mock! {
        AddressManager {}
        impl AddressManager for AddressManager {
            fn get_leader_address(&self) -> Result<String>;
            fn get_follower_addresses(&self) -> Result<Vec<String>>;
            fn get_all_addresses(&self) -> Result<Vec<String>>;
        }
    }

    #[test]
    fn test_calculate_hash() {
        let key1 = vec![1, 2, 3, 4, 5];
        let key2 = vec![5, 4, 3, 2, 1];
        let key3 = vec![1, 2, 3, 4, 5];

        let hash1 = calculate_hash(&key1);
        let hash2 = calculate_hash(&key2);
        let hash3 = calculate_hash(&key3);

        assert_ne!(hash1, hash2);
        assert_eq!(hash1, hash3);
    }

    #[tokio::test]
    async fn test_get_random_address() {
        let mut mock_address_manager = MockAddressManager::new();
        mock_address_manager.expect_get_all_addresses().returning(|| Ok(vec!["localhost:50051".to_string(), "localhost:50052".to_string()]));

        let address_managers = vec![Box::new(mock_address_manager) as Box<dyn AddressManager>];
        let address_managers = Arc::new(Mutex::new(address_managers));

        let address = get_random_address(&address_managers, 0).await.unwrap();
        assert!(address == "localhost:50051" || address == "localhost:50052");
    }

    #[tokio::test]
    async fn test_get_random_address_no_addresses() {
        let mut mock_address_manager = MockAddressManager::new();
        mock_address_manager.expect_get_all_addresses().returning(|| Ok(vec![]));

        let address_managers = vec![Box::new(mock_address_manager) as Box<dyn AddressManager>];
        let address_managers = Arc::new(Mutex::new(address_managers));

        let result = get_random_address(&address_managers, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_random_address_error() {
        let mut mock_address_manager = MockAddressManager::new();
        mock_address_manager.expect_get_all_addresses().returning(move || Err(anyhow!("error")));

        let address_managers = vec![Box::new(mock_address_manager) as Box<dyn AddressManager>];
        let address_managers = Arc::new(Mutex::new(address_managers));

        let result = get_random_address(&address_managers, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_partitioned_leader_address() {
        let mut mock_address_manager = MockAddressManager::new();
        mock_address_manager.expect_get_leader_address().returning(|| Ok("localhost:50051".to_string()));

        let address_managers = vec![Box::new(mock_address_manager) as Box<dyn AddressManager>];
        let address_managers = Arc::new(Mutex::new(address_managers));

        let address = get_partitioned_leader_address(&address_managers, 0).await.unwrap();
        assert_eq!(address, "localhost:50051");
    }

    #[tokio::test]
    async fn test_get_partitioned_leader_address_error() {
        let mut mock_address_manager = MockAddressManager::new();
        mock_address_manager.expect_get_leader_address().returning(move || Err(anyhow!("error")));

        let address_managers = vec![Box::new(mock_address_manager) as Box<dyn AddressManager>];
        let address_managers = Arc::new(Mutex::new(address_managers));

        let result = get_partitioned_leader_address(&address_managers, 0).await;
        assert!(result.is_err());
    }
}
