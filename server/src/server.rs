use std::sync::Arc;
use log::{error, info};
use prost::Message;
use prost_types::Value;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tokio::sync::mpsc::channel;

use configmanager::ConfigManager;
use indexengine::index::Index;
use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_server::KeyValueService;
use crate::error::ServerError;

use crate::server::key_value_store::key_value_service_client::KeyValueServiceClient;

pub mod key_value_store {
    tonic::include_proto!("server");
}

pub struct KeyValueStoreImpl {
    index_engine: Mutex<Box<dyn Index<Vec<u8>, Vec<u8>>>>,
    tx: Sender<Replication>,
}

#[derive(Clone, Debug)]
enum Action {
    Add,
    Update,
    Delete,
}

#[derive(Clone, Debug)]
struct Replication {
    action: Action,
    key_value: KeyValue,
}

impl KeyValueStoreImpl {
    pub async fn new(index_engine: Box<dyn Index<Vec<u8>, Vec<u8>>>, config_manager: Box<dyn ConfigManager>) -> Self {
        let (tx, rx) = channel::<Replication>(1000);
        let config_manager = Arc::new(Mutex::new(config_manager));
        start_replicator(rx, config_manager.clone()).await;

        Self {
            index_engine: Mutex::new(index_engine),
            tx,
        }
    }
}

async fn start_replicator(mut rx: Receiver<Replication>, config_manager: Arc<Mutex<Box<dyn ConfigManager>>>) {
    let config_manager = config_manager.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let config_manager_unlocked = config_manager.lock().await;
            if !config_manager_unlocked.is_leader() {
                info!("Not leader, skipping replication");
                continue;
            }

            info!("Got replication message: {:?}", message);
            let follower_addresses = match config_manager_unlocked.get_follower_addresses() {
                Ok(follower_addresses) => follower_addresses,
                Err(e) => {
                    log::error!("Failed to get follower addresses: {:?}", e);
                    continue;
                }
            };
            info!("attempt to replicate to followers: {:?}", follower_addresses);

            for follower_address in follower_addresses {
                let message = message.clone();
                let mut client = match KeyValueServiceClient::connect(follower_address).await {
                    Ok(client) => client,
                    Err(e) => {
                        log::error!("Failed to connect to follower: {:?}", e);
                        continue;
                    }
                };
                match message.action {
                    Action::Add => {
                        let request = Request::new(CreateRequest {
                            key_value: Some(message.key_value),
                        });
                        match client.create(request).await {
                            Ok(_) => {
                                info!("Successfully replicated create to follower");
                            }
                            Err(e) => {
                                error!("Failed to replicate create to follower: {:?}", e);
                            }
                        }
                    }
                    Action::Update => {
                        let request = Request::new(UpdateRequest {
                            key_value: Some(message.key_value),
                        });
                        match client.update(request).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Failed to replicate update to follower: {:?}", e);
                            }
                        }
                    }
                    Action::Delete => {
                        let request = Request::new(DeleteRequest {
                            key: message.key_value.key,
                        });
                        match client.delete(request).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Failed to replicate delete to follower: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    });
}

#[tonic::async_trait]
impl KeyValueService for KeyValueStoreImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        println!("Got a request: {:?}", request);

        let key_val = request.into_inner().key
            .ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let mut index_engine = self.index_engine.lock().await;
        let document = index_engine.search(&key_bytes).map_err(ServerError::from)?;

        let bytes = prost::bytes::Bytes::from(document.id);
        let key = Value::decode(bytes).map_err(ServerError::from)?;
        let bytes = prost::bytes::Bytes::from(document.value);
        let value = Value::decode(bytes).map_err(ServerError::from)?;

        let reply = GetResponse {
            key_value: Option::from(KeyValue {
                key: key.into(),
                value: value.into(),
            })
        };

        Ok(Response::new(reply))
    }

    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        println!("Got a request: {:?}", request);

        let key_value = request.into_inner().key_value
            .ok_or_else(|| ServerError::InvalidArgument("key_value must be set".to_string()))?;

        let replication = Replication {
            action: Action::Add,
            key_value: key_value.clone(),
        };

        let key_val = key_value.key.ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let value_val = key_value.value.ok_or_else(|| ServerError::InvalidArgument("value must be set".to_string()))?;
        let value_bytes = value_val.encode_to_vec();

        let mut index_engine = self.index_engine.lock().await;
        index_engine.insert(indexengine::index::Document {
            id: key_bytes,
            value: value_bytes,
        }).map_err(ServerError::from)?;

        match self.tx.send(replication).await {
            Ok(_) => {
                info!("Successfully sent replication message");
            }
            Err(e) => {
                error!("Failed to send replication message: {:?}", e);
            }
        }

        let reply = CreateResponse {
            key_value: Option::from(KeyValue {
                key: key_val.into(),
                value: value_val.into(),
            })
        };

        Ok(Response::new(reply))
    }

    async fn update(&self, _request: Request<UpdateRequest>) -> Result<Response<UpdateResponse>, Status> {
        todo!()
    }

    async fn delete(&self, _request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        todo!()
    }
}

pub struct ServerError2(anyhow::Error);

impl From<anyhow::Error> for ServerError2 {
    fn from(err: anyhow::Error) -> Self {
        ServerError2(err)
    }
}

impl From<ServerError2> for Status {
    fn from(err: ServerError2) -> Self {
        Status::internal(err.0.to_string())
    }
}
