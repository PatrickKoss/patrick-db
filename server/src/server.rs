use std::sync::Arc;

use log::{error, info};
use prost::bytes::Bytes;
use prost::Message;
use prost_types::Value;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use configmanager::ConfigManager;
use indexengine::index::{Document, Index};
use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_client::KeyValueServiceClient;
use key_value_store::key_value_service_server::KeyValueService;

use crate::error::ServerError;

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

    async fn send_replication(&self, replication: Replication) {
        match self.tx.send(replication).await {
            Ok(_) => {
                info!("Successfully sent replication message");
            }
            Err(e) => {
                error!("Failed to send replication message: {:?}", e);
            }
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
                    error!("Failed to get follower addresses: {:?}", e);
                    continue;
                }
            };
            info!("attempt to replicate to followers: {:?}", follower_addresses);

            for follower_address in follower_addresses {
                let message = message.clone();
                let mut client = match KeyValueServiceClient::connect(follower_address).await {
                    Ok(client) => client,
                    Err(e) => {
                        error!("Failed to connect to follower: {:?}", e);
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
                                error!("Failed to replicate update to follower: {:?}", e);
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
                                error!("Failed to replicate delete to follower: {:?}", e);
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
        let key_val = request.into_inner().key
            .ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let mut index_engine = self.index_engine.lock().await;
        let document = index_engine.search(&key_bytes).map_err(ServerError::from)?;

        let bytes = Bytes::from(document.id);
        let key = Value::decode(bytes).map_err(ServerError::from)?;
        let bytes = Bytes::from(document.value);
        let value = Value::decode(bytes).map_err(ServerError::from)?;

        let reply = GetResponse {
            key_value: Some(KeyValue {
                key: key.into(),
                value: value.into(),
            })
        };

        Ok(Response::new(reply))
    }

    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
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
        index_engine.insert(Document {
            id: key_bytes,
            value: value_bytes,
        }).map_err(ServerError::from)?;

        self.send_replication(replication).await;

        let reply = CreateResponse {
            key_value: Some(KeyValue {
                key: key_val.into(),
                value: value_val.into(),
            })
        };

        Ok(Response::new(reply))
    }

    async fn update(&self, request: Request<UpdateRequest>) -> Result<Response<UpdateResponse>, Status> {
        let key_value = request.into_inner().key_value
            .ok_or_else(|| ServerError::InvalidArgument("key_value must be set".to_string()))?;

        let replication = Replication {
            action: Action::Update,
            key_value: key_value.clone(),
        };

        let key_val = key_value.key.ok_or_else(|| ServerError::InvalidArgument("key must be set".to_string()))?;
        let key_bytes = key_val.encode_to_vec();

        let value_val = key_value.value.ok_or_else(|| ServerError::InvalidArgument("value must be set".to_string()))?;
        let value_bytes = value_val.encode_to_vec();

        let mut index_engine = self.index_engine.lock().await;
        index_engine.update(&key_bytes.clone(), Document {
            id: key_bytes,
            value: value_bytes,
        }).map_err(ServerError::from)?;

        self.send_replication(replication).await;

        let reply = UpdateResponse {
            key_value: Some(KeyValue {
                key: key_val.into(),
                value: value_val.into(),
            })
        };

        Ok(Response::new(reply))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key
            .ok_or_else(|| ServerError::InvalidArgument("key_value must be set".to_string()))?;
        let key_bytes = key.encode_to_vec();

        let replication = Replication {
            action: Action::Delete,
            key_value: KeyValue {
                key: Some(key.clone()),
                value: None,
            },
        };

        let mut index_engine = self.index_engine.lock().await;
        index_engine.delete(&key_bytes).map_err(ServerError::from)?;

        self.send_replication(replication).await;

        let reply = DeleteResponse {
            key_value: Some(KeyValue {
                key: key.into(),
                value: None,
            })
        };

        Ok(Response::new(reply))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    #[cfg(test)]
    use mockall::mock;
    use mockall::predicate;
    use prost_types::value::Kind;

    use super::*;

    mock! {
        IndexImpl{}
        impl Index<Vec<u8>, Vec<u8>> for IndexImpl {
            fn insert(&mut self, document: Document<Vec<u8>, Vec<u8>>) -> Result<()>;
            fn update(&mut self, key: &Vec<u8>, document: Document<Vec<u8>, Vec<u8>>) -> Result<()>;
            fn delete(&mut self, key: &Vec<u8>) -> Result<()>;
            fn search(&mut self, key: &Vec<u8>) -> Result<Document<Vec<u8>, Vec<u8>>>;
        }
    }

    mock! {
        ConfigManagerImpl{}
        impl ConfigManager for ConfigManagerImpl {
            fn is_leader(&self) -> bool;
            fn get_follower_addresses(&self) -> Result<Vec<String>>;
            fn get_leader_address(&self) -> Result<String>;
            fn get_name(&self) -> String;
        }
    }

    #[tokio::test]
    async fn test_get() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };
        let key_bytes = key.encode_to_vec();
        let key_bytes_clone = key.encode_to_vec();

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_search()
            .with(predicate::always())
            .returning(move |_| Ok(Document {
                id: key_bytes.clone(),
                value: key_bytes_clone.clone(),
            }));
        let mut mock_config_manager = MockConfigManagerImpl::new();
        mock_config_manager.expect_get_follower_addresses()
            .returning(|| Ok(vec![]));

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(GetRequest {
            key: Some(key),
        });

        let response = service.get(request).await.unwrap();
        let key_value = response.into_inner().key_value.unwrap();
        let key = match key_value.key.unwrap().kind {
            Some(Kind::StringValue(s)) => s,
            _ => String::from(""),
        };
        assert_eq!(key, "test".to_string());
    }

    #[tokio::test]
    async fn test_get_no_key() {
        let mock_index = MockIndexImpl::new();
        let mock_config_manager = MockConfigManagerImpl::new();

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(GetRequest {
            key: None,
        });

        let response = service.get(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_search()
            .with(predicate::always())
            .returning(move |_| Err(indexengine::index::IndexError::NotFound.into()));
        let mut mock_config_manager = MockConfigManagerImpl::new();
        mock_config_manager.expect_get_follower_addresses()
            .returning(|| Ok(vec![]));

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(GetRequest {
            key: Some(key),
        });

        let response = service.get(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_create() {
        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_insert()
            .with(predicate::always())
            .returning(move |_| Ok(()));
        let mock_config_manager = MockConfigManagerImpl::new();
        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(CreateRequest {
            key_value: Some(KeyValue {
                key: Some(Value { kind: Some(Kind::StringValue("test".to_string())) }),
                value: Some(Value { kind: Some(Kind::StringValue("value".to_string())) }),
            }),
        });

        let response = service.create(request).await.unwrap();

        let key_value = response.into_inner().key_value.unwrap();
        let key = match key_value.key.unwrap().kind {
            Some(Kind::StringValue(s)) => s,
            _ => String::from(""),
        };
        assert_eq!(key, "test".to_string());
    }

    #[tokio::test]
    async fn test_create_no_key() {
        let mock_index = MockIndexImpl::new();
        let mock_config_manager = MockConfigManagerImpl::new();

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(CreateRequest {
            key_value: None,
        });

        let response = service.create(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);

        let request = Request::new(CreateRequest {
            key_value: Some(KeyValue {
                key: None,
                value: Some(Value { kind: Some(Kind::StringValue("value".to_string())) }),
            }),
        });

        let response = service.create(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_create_already_exists() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_insert()
            .with(predicate::always())
            .returning(move |_| Err(indexengine::index::IndexError::AlreadyExists.into()));
        let mut mock_config_manager = MockConfigManagerImpl::new();
        mock_config_manager.expect_get_follower_addresses()
            .returning(|| Ok(vec![]));

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(CreateRequest {
            key_value: Some(KeyValue {
                key: Some(key),
                value: Some(Value { kind: Some(Kind::StringValue("value".to_string())) }),
            }),
        });

        let response = service.create(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::AlreadyExists);
    }

    #[tokio::test]
    async fn test_update() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };
        let key_bytes = key.encode_to_vec();

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_update()
            .with(predicate::eq(key_bytes.clone()), predicate::always())
            .returning(move |_, _| Ok(()));
        let mock_config_manager = MockConfigManagerImpl::new();
        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(UpdateRequest {
            key_value: Some(KeyValue {
                key: Some(Value { kind: Some(Kind::StringValue("test".to_string())) }),
                value: Some(Value { kind: Some(Kind::StringValue("value".to_string())) }),
            }),
        });

        let response = service.update(request).await.unwrap();

        let key_value = response.into_inner().key_value.unwrap();
        let key = match key_value.key.unwrap().kind {
            Some(Kind::StringValue(s)) => s,
            _ => String::from(""),
        };
        assert_eq!(key, "test".to_string());
    }

    #[tokio::test]
    async fn test_update_no_key() {
        let mock_index = MockIndexImpl::new();
        let mock_config_manager = MockConfigManagerImpl::new();

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(UpdateRequest {
            key_value: None,
        });

        let response = service.update(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);

        let request = Request::new(UpdateRequest {
            key_value: Some(KeyValue {
                key: None,
                value: Some(Value { kind: Some(Kind::StringValue("value".to_string())) }),
            }),
        });

        let response = service.update(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_update_not_found() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_update()
            .with(predicate::always(), predicate::always())
            .returning(move |_, _| Err(indexengine::index::IndexError::NotFound.into()));
        let mut mock_config_manager = MockConfigManagerImpl::new();
        mock_config_manager.expect_get_follower_addresses()
            .returning(|| Ok(vec![]));

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(UpdateRequest {
            key_value: Some(KeyValue {
                key: Some(key),
                value: Some(Value { kind: Some(Kind::StringValue("value".to_string())) }),
            }),
        });

        let response = service.update(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_delete() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };
        let key_bytes = key.encode_to_vec();

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_delete()
            .with(predicate::eq(key_bytes))
            .returning(move |_| Ok(()));
        let mock_config_manager = MockConfigManagerImpl::new();
        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(DeleteRequest {
            key: Some(Value { kind: Some(Kind::StringValue("test".to_string())) }),
        });

        let response = service.delete(request).await.unwrap();

        let key_value = response.into_inner().key_value.unwrap();
        let key = match key_value.key.unwrap().kind {
            Some(Kind::StringValue(s)) => s,
            _ => String::from(""),
        };
        assert_eq!(key, "test".to_string());
    }

    #[tokio::test]
    async fn test_delete_no_key() {
        let mock_index = MockIndexImpl::new();
        let mock_config_manager = MockConfigManagerImpl::new();

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(DeleteRequest {
            key: None,
        });

        let response = service.delete(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        let key = Value {
            kind: Some(Kind::StringValue("test".to_string())),
        };

        let mut mock_index = MockIndexImpl::new();
        mock_index.expect_delete()
            .with(predicate::always())
            .returning(move |_| Err(indexengine::index::IndexError::NotFound.into()));
        let mut mock_config_manager = MockConfigManagerImpl::new();
        mock_config_manager.expect_get_follower_addresses()
            .returning(|| Ok(vec![]));

        let service = KeyValueStoreImpl::new(Box::new(mock_index), Box::new(mock_config_manager)).await;

        let request = Request::new(DeleteRequest {
            key: Some(key),
        });

        let response = service.delete(request).await;
        assert!(response.is_err());
        assert_eq!(response.err().unwrap().code(), tonic::Code::NotFound);
    }
}
