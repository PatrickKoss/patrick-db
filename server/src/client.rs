use prost_types::Value;
use prost_types::value::Kind;

use key_value_store::GetRequest;
use key_value_store::key_value_service_client::KeyValueServiceClient;

pub mod key_value_store {
    tonic::include_proto!("server");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KeyValueServiceClient::connect("http://[::1]:50051").await?;

    let value = Value {
        kind: Some(Kind::StringValue("value".to_string())),
    };

    let request = tonic::Request::new(GetRequest {
        key: value.into(),
    });

    let response = client.get(request).await?;

    println!("RESPONSE={:?}", response);

    let key_value = response.into_inner().key_value.unwrap();
    println!("KEY={:?}", String::from_utf8_lossy(&key_value.key.unwrap().value));
    println!("VALUE={:?}", String::from_utf8_lossy(&key_value.value.unwrap().value));

    Ok(())
}
