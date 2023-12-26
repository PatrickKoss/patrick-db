extern crate env_logger;
extern crate uuid;
extern crate zookeeper;

use std::{env, thread, time::Duration};

use crate::config_manager::ConfigManager;

mod config_manager;

const LATCH_PATH: &str = "/latch-ex";
const SERVICE_REGISTRY_PATH: &str = "/services";

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    env::var(key).unwrap_or_else(|_| "localhost:2181".to_string())
}

fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let service_url = env::var("SERVICE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let zk_urls = zk_server_urls();
    log::info!("connecting to {}", &zk_urls);

    let zookeeper_config_manager = config_manager::ZooKeeperConfigManager::new(SERVICE_REGISTRY_PATH, LATCH_PATH, &service_url, &zk_urls).unwrap();

    loop {
        if zookeeper_config_manager.is_leader() {
            log::info!("{:?} is the leader", zookeeper_config_manager.get_name());
        } else {
            log::info!("{:?} is a follower", zookeeper_config_manager.get_name());
        }

        match zookeeper_config_manager.get_leader_address() {
            Ok(leader_address) => log::info!("leader: {}", leader_address),
            Err(e) => log::error!("Error getting leader address: {:?}", e),
        }

        match zookeeper_config_manager.get_follower_addresses() {
            Ok(follower_addresses) => {
                for follower_address in follower_addresses {
                    log::info!("follower: {}", follower_address);
                }
            }
            Err(e) => log::error!("Error getting follower addresses: {:?}", e),
        }

        thread::sleep(Duration::from_millis(10000));
    }
}
