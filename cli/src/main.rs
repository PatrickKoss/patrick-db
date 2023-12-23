use std::env;

use anyhow::Result;
use clap::{Parser, Subcommand};

use indexengine::index::Index;
use indexengine::no_index::NoIndex;

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
    #[arg(short, long, default_value = "patrick.db")]
    file: String,
    #[command(subcommand)]
    action: Action,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let file_name = env::var("FILE").ok().unwrap_or(args.file);
    let file_handler = storageengine::file_handler::FileHandlerImpl::new(&file_name)?;
    let operations = storageengine::operations::DbOperationsImpl::new(Box::new(file_handler));
    let mut index_engine = NoIndex::new(Box::new(operations));

    match args.action {
        Action::Add { key, value } => {
            let document = indexengine::index::Document {
                id: key.clone(),
                value: value.clone().into_bytes(),
            };
            match index_engine.insert(document) {
                Ok(_) => println!("inserted {}, {}", key, value),
                Err(e) => println!("failed to insert: {}", e),
            }
        }
        Action::Get { key } => {
            match index_engine.search(&key) {
                Ok(document) => println!("{}, {}", key, String::from_utf8(document.value).unwrap()),
                Err(e) => println!("failed to get: {}", e),
            }
        }
        Action::Delete { key } => {
            match index_engine.delete(&key) {
                Ok(_) => println!("deleted {}", key),
                Err(e) => println!("failed to delete: {}", e),
            }
        }
        Action::Update { key, value } => {
            match index_engine.update(&key, indexengine::index::Document {
                id: key.clone(),
                value: value.clone().into_bytes(),
            }) {
                Ok(_) => println!("updated {}, {}", key, value),
                Err(e) => println!("failed to update: {}", e),
            }
        }
    }

    Ok(())
}

