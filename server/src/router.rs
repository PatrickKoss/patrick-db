use std::sync::Arc;

use prost::Message;
use rand::Rng;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use configmanager::AddressManager;
use configmanager::ConfigManager;
use indexengine::index::Index;
use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_client::KeyValueServiceClient;
use key_value_store::key_value_service_server::KeyValueService;

pub mod key_value_store {
    tonic::include_proto!("server");
}

pub struct KeyValueServiceRouter {
    address_manager: Arc<Mutex<Box<dyn AddressManager>>>,
}

impl KeyValueServiceRouter {
    pub fn new(address_manager: Box<dyn AddressManager>) -> Self {
        Self {
            address_manager: Arc::new(Mutex::new(address_manager))
        }
    }
}

#[tonic::async_trait]
impl KeyValueService for KeyValueServiceRouter {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let address_manager = self.address_manager.lock().await;
        let addresses = address_manager.get_all_addresses().map_err(|_| Status::internal("Could not get addresses"))?; // TODO: better error handling

        // random number between 0 and addresses.len()
        let random_address_index = rand::thread_rng().gen_range(0..addresses.len());
        let address = addresses[random_address_index].clone();

        let mut client = KeyValueServiceClient::connect(address).await.map_err(|_| Status::internal("Could not connect to address"))?;
        client.get(request).await
    }

    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        todo!()
    }

    async fn update(&self, request: Request<UpdateRequest>) -> Result<Response<UpdateResponse>, Status> {
        todo!()
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        todo!()
    }
}

fn main() {}
