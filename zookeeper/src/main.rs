extern crate env_logger;
extern crate uuid;
extern crate zookeeper;

use std::{env, sync::Arc, thread, time::Duration};

use uuid::Uuid;
use zookeeper::{recipes::leader::LeaderLatch, WatchedEvent, Watcher, ZooKeeper, CreateMode, Acl};

const LATCH_PATH: &str = "/latch-ex";
const SERVICE_REGISTRY_PATH: &str = "/services";

struct NoopWatcher;

impl Watcher for NoopWatcher {
    fn handle(&self, e: WatchedEvent) {
        println!("WatchedEvent: {:?}", e);
    }
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    env::var(key).unwrap_or_else(|_| "localhost:2181".to_string())
}

fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let service_url = env::var("SERVICE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let zk_urls = zk_server_urls();
    log::info!("connecting to {}", &zk_urls);

    let zk = match ZooKeeper::connect(&*zk_urls, Duration::from_millis(2500), NoopWatcher) {
        Ok(zk) => zk,
        Err(e) => {
            log::error!("Failed to connect to ZooKeeper: {:?}", e);
            return;
        }
    };

    let zk_arc = Arc::new(zk);

    // Ensure the parent node for service registry exists
    let exists = zk_arc.exists(SERVICE_REGISTRY_PATH, false).unwrap();
    if exists.is_none() {
        // Parent node doesn't exist, so create it
        zk_arc.create(SERVICE_REGISTRY_PATH, Vec::from(b""), Acl::open_unsafe().clone(), CreateMode::Persistent).unwrap();
    }

    // Register service
    let service_id = Uuid::new_v4().to_string();
    let service_path = format!("{}/{}", SERVICE_REGISTRY_PATH, service_id);
    let acls = Acl::open_unsafe().clone();
    zk_arc.create(&service_path, Vec::from(service_url), acls, CreateMode::Ephemeral).unwrap();

    log::info!("Service registered with ID: {}", service_id);

    // LeaderLatch setup
    let latch = LeaderLatch::new(zk_arc.clone(), service_id.clone(), LATCH_PATH.into());
    latch.start().unwrap();

    loop {
        if latch.has_leadership() {
            log::info!("{:?} is the leader", service_id);
        } else {
            log::info!("{:?} is a follower", service_id);
        }

        // Discover registered services
        if let Ok(services) = zk_arc.get_children(SERVICE_REGISTRY_PATH, false) {
            for service_id in services {
                let service_path = format!("{}/{}", SERVICE_REGISTRY_PATH, service_id);
                match zk_arc.get_data(&service_path, false) {
                    Ok((data, _)) => {
                        // Assuming the service URL is stored as a String
                        let service_url = String::from_utf8_lossy(&data);
                        log::info!("Discovered service: {}, URL: {}", service_id, service_url);
                    }
                    Err(e) => log::error!("Error getting data for service {}: {:?}", service_id, e),
                }
            }
        }

        thread::sleep(Duration::from_millis(10000));
    }
}
