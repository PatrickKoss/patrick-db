use key_value_store::key_value_service_client::KeyValueServiceClient;
use key_value_store::GetRequest;
use prost_types::Any;
use std::string::String;

pub mod key_value_store {
    tonic::include_proto!("server");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueServiceClient::connect("http://[::1]:50051").await?;

    let key = Any {
        type_url: "type_url".to_string(),
        value: String::from("key").into_bytes(),
    };

    let request = tonic::Request::new(GetRequest {
        key: Some(key),
    });

    let response = client.get(request).await?;

    println!("RESPONSE={:?}", response);

    let key_value = response.into_inner().key_value.unwrap();
    println!("KEY={:?}", String::from_utf8_lossy(&key_value.key.unwrap().value));
    println!("VALUE={:?}", String::from_utf8_lossy(&key_value.value.unwrap().value));

    Ok(())
}
