use prost::Message;
use prost_types::Value;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use configmanager::ConfigManager;
use indexengine::index::Index;
use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_server::KeyValueService;

pub mod key_value_store {
    tonic::include_proto!("server");
}

pub struct KeyValueStoreImpl {
    index_engine: Mutex<Box<dyn Index<Vec<u8>, Vec<u8>>>>,
    config_manager: Mutex<Box<dyn ConfigManager>>,
}

impl KeyValueStoreImpl {
    pub fn new(index_engine: Box<dyn Index<Vec<u8>, Vec<u8>>>, config_manager: Box<dyn ConfigManager>) -> Self {
        Self {
            index_engine: Mutex::new(index_engine),
            config_manager: Mutex::new(config_manager),
        }
    }
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
                if let Some(index_error) = e.downcast_ref::<indexengine::index::IndexError>() {
                    return match index_error {
                        indexengine::index::IndexError::NotFound => {
                            Err(Status::not_found("key not found"))
                        }
                        _ => {
                            Err(Status::internal(e.to_string()))
                        }
                    }
                } else {
                    return Err(Status::internal(e.to_string()));
                }
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
                if let Some(index_error) = e.downcast_ref::<indexengine::index::IndexError>() {
                    return match index_error {
                        indexengine::index::IndexError::AlreadyExists => {
                            Err(Status::already_exists("document already exists"))
                        }
                        _ => {
                            Err(Status::internal(e.to_string()))
                        }
                    }
                } else {
                    return Err(Status::internal(e.to_string()));
                }
            }
        };

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
