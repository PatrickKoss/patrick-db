use tonic::{Request, Response, Status, transport::Server};

use key_value_store::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, CreateRequest, CreateResponse, UpdateRequest, UpdateResponse};
use key_value_store::key_value_service_server::{KeyValueService, KeyValueServiceServer};
use prost_types::Value;
use std::string::String;
use prost_types::value::Kind;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let server = KeyValueStoreImpl::default();

    Server::builder()
        .add_service(KeyValueServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
