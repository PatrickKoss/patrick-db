use std::env;
use clap::{Parser, Subcommand};
use prost_types::Value;
use prost_types::value::Kind;
use anyhow::Result;

use key_value_store::{GetRequest, CreateRequest, UpdateRequest, DeleteRequest};
use key_value_store::key_value_service_client::KeyValueServiceClient;

pub mod key_value_store {
    tonic::include_proto!("server");
}

#[derive(Subcommand, PartialEq, Debug)]
enum Action {
    Add { key: String, value: String },
    Update { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "http://[::1]:50051")]
    server_url: String,
    #[command(subcommand)]
    action: Action,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let server_url = env::var("SERVER_URL").ok().unwrap_or(args.server_url);
    let mut client = KeyValueServiceClient::connect(server_url).await?;

    match args.action {
        Action::Add { key, value } => {
            let key = Value {
                kind: Some(Kind::StringValue(key)),
            };
            let value = Value {
                kind: Some(Kind::StringValue(value)),
            };

            let request = tonic::Request::new(CreateRequest {
                key_value: Some(key_value_store::KeyValue {
                    key: Some(key),
                    value: Some(value),
                }),
            });

            let response = client.create(request).await?;
            println!("RESPONSE={:?}", response);
            let key_value = response.into_inner().key_value.unwrap();
            print_key_value(key_value);
        }
        Action::Get { key } => {
            let key = Value {
                kind: Some(Kind::StringValue(key)),
            };

            let request = tonic::Request::new(GetRequest {
                key: key.into(),
            });

            let response = client.get(request).await?;
            println!("RESPONSE={:?}", response);
            let key_value = response.into_inner().key_value.unwrap();
            print_key_value(key_value);
        }
        Action::Delete { key } => {
            let key = Value {
                kind: Some(Kind::StringValue(key)),
            };

            let request = tonic::Request::new(DeleteRequest {
                key: key.into(),
            });

            let response = client.delete(request).await?;
            println!("RESPONSE={:?}", response);
            let key_value = response.into_inner().key_value.unwrap();
            print_key_value(key_value);
        }
        Action::Update { key, value } => {
            let key = Value {
                kind: Some(Kind::StringValue(key)),
            };
            let value = Value {
                kind: Some(Kind::StringValue(value)),
            };

            let request = tonic::Request::new(UpdateRequest {
                key_value: Some(key_value_store::KeyValue {
                    key: Some(key),
                    value: Some(value),
                }),
            });

            let response = client.update(request).await?;
            println!("RESPONSE={:?}", response);
            let key_value = response.into_inner().key_value.unwrap();
            print_key_value(key_value);
        }
    }

    Ok(())
}

fn print_key_value(key_value: key_value_store::KeyValue) {
    let key = match key_value.key.unwrap().kind {
        Some(Kind::StringValue(s)) => s,
        _ => String::from(""),
    };
    println!("KEY={:?}", &key);

    let value = match key_value.value.unwrap().kind {
        Some(Kind::StringValue(s)) => s,
        _ => String::from(""),
    };
    println!("VALUE={:?}", &value);
}
