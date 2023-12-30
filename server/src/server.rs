use prost::Message;
use prost_types::Value;
use prost_types::value::Kind;
use tonic::{Request, Response, Status};

use key_value_store::{CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_server::KeyValueService;

pub mod key_value_store {
    tonic::include_proto!("server");
}


#[derive(Debug, Default)]
pub struct KeyValueStoreImpl {}

#[tonic::async_trait]
impl KeyValueService for KeyValueStoreImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        println!("Got a request: {:?}", request);
        let key = Value {
            kind: Some(Kind::StringValue("key".to_string())),
        };

        let value = Value {
            kind: Some(Kind::StringValue("value".to_string())),
        };

        let reply = GetResponse {
            key_value: Option::from(KeyValue {
                key: key.into(),
                value: value.into(),
            })
        };

        Ok(Response::new(reply))
    }

    async fn create(&self, _request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        println!("Got a request: {:?}", _request);

        let v = _request.into_inner().key_value.unwrap().value.unwrap();
        v.encode_to_vec();

        let key = Value {
            kind: Some(Kind::StringValue("key".to_string())),
        };

        let value = Value {
            kind: Some(Kind::StringValue("value".to_string())),
        };

        let reply = CreateResponse {
            key_value: Option::from(KeyValue {
                key: key.into(),
                value: value.into(),
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
