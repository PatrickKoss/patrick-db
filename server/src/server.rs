use std::sync::Arc;
use prost::Message;
use prost_types::Value;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use configmanager::ConfigManager;
use indexengine::index::Index;
use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_server::KeyValueService;

use crate::server::key_value_store::key_value_service_client::KeyValueServiceClient;

pub mod key_value_store {
    tonic::include_proto!("server");
}

pub struct KeyValueStoreImpl {
    index_engine: Mutex<Box<dyn Index<Vec<u8>, Vec<u8>>>>,
    config_manager: Arc<Mutex<Box<dyn ConfigManager>>>,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Replication>(1000);
        let config_manager = Arc::new(Mutex::new(config_manager));
        start_replicator(rx, config_manager.clone()).await;

        Self {
            index_engine: Mutex::new(index_engine),
            config_manager,
            tx,
        }
    }
}

async fn start_replicator(mut rx: Receiver<Replication>, config_manager: Arc<Mutex<Box<dyn ConfigManager>>>) {
    let config_manager = config_manager.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            log::info!("Got replication message: {:?}", message);
            let follower_addresses = match config_manager.lock().await.get_follower_addresses() {
                Ok(follower_addresses) => follower_addresses,
                Err(e) => {
                    log::error!("Failed to get follower addresses: {:?}", e);
                    continue;
                }
            };
            log::info!("attempt to replicate to followers: {:?}", follower_addresses);

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
                                log::info!("Successfully replicated create to follower");
                            }
                            Err(e) => {
                                log::error!("Failed to replicate create to follower: {:?}", e);
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

        let key_val = match request.into_inner().key {
            Some(key_val) => key_val,
            None => return Err(Status::invalid_argument("key must be set")),
        };
        let key_bytes = key_val.encode_to_vec();
        let mut index_engine = self.index_engine.lock().await;
        let document = match index_engine.search(&key_bytes) {
            Ok(document) => document,
            Err(e) => {
                return if let Some(index_error) = e.downcast_ref::<indexengine::index::IndexError>() {
                    match index_error {
                        indexengine::index::IndexError::NotFound => {
                            Err(Status::not_found("key not found"))
                        }
                        _ => {
                            Err(Status::internal(e.to_string()))
                        }
                    }
                } else {
                    Err(Status::internal(e.to_string()))
                };
            }
        };

        let bytes = prost::bytes::Bytes::from(document.id);
        let key = match Value::decode(bytes) {
            Ok(key) => key,
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        };
        let bytes = prost::bytes::Bytes::from(document.value);
        let value = match Value::decode(bytes) {
            Ok(value) => value,
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        };

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
        let config_manager = self.config_manager.lock().await;
        if !config_manager.is_leader() {
            return Err(Status::unavailable("not a leader"));
        }

        let key_value = match request.into_inner().key_value {
            Some(key_value) => key_value,
            None => return Err(Status::invalid_argument("key_value must be set")),
        };

        let replication = Replication {
            action: Action::Add,
            key_value: key_value.clone(),
        };

        let key_val = match key_value.key {
            Some(key_val) => key_val,
            None => return Err(Status::invalid_argument("key must be set")),
        };
        let key_bytes = key_val.encode_to_vec();

        let value_val = match key_value.value {
            Some(value_val) => value_val,
            None => return Err(Status::invalid_argument("value must be set")),
        };
        let value_bytes = value_val.encode_to_vec();

        let mut index_engine = self.index_engine.lock().await;
        match index_engine.insert(indexengine::index::Document {
            id: key_bytes,
            value: value_bytes,
        }) {
            Ok(_) => {}
            Err(e) => {
                return if let Some(index_error) = e.downcast_ref::<indexengine::index::IndexError>() {
                    match index_error {
                        indexengine::index::IndexError::AlreadyExists => {
                            Err(Status::already_exists("document already exists"))
                        }
                        _ => {
                            Err(Status::internal(e.to_string()))
                        }
                    }
                } else {
                    Err(Status::internal(e.to_string()))
                };
            }
        };

        match self.tx.send(replication).await {
            Ok(_) => {
                log::info!("Successfully sent replication message");
            }
            Err(e) => {
                log::error!("Failed to send replication message: {:?}", e);
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
