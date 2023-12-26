extern crate uuid;
extern crate zookeeper;

use std::{sync::Arc, time::Duration};
use std::sync::RwLock;

use anyhow::Result;
use uuid::Uuid;
use zookeeper::{Acl, CreateMode, recipes::leader::LeaderLatch, WatchedEvent, Watcher, ZooKeeper};

pub trait ConfigManager {
    fn is_leader(&self) -> bool;
    fn get_leader_address(&self) -> Result<String>;
    fn get_follower_addresses(&self) -> Result<Vec<String>>;
    fn get_name(&self) -> String;
}

pub struct ZooKeeperConfigManager {
    service_id: String,
    latch: LeaderLatch,
    instances: Arc<RwLock<Vec<Instance>>>,
}

pub struct Instance {
    pub id: String,
    pub address: String,
}

struct NoopWatcher;

impl Watcher for NoopWatcher {
    fn handle(&self, _: WatchedEvent) {}
}

impl ZooKeeperConfigManager {
    pub fn new(service_registry_path: &str, latch_path: &str, instance_address: &str, zookeeper_urls: &str) -> Result<Self> {
        let zk = match ZooKeeper::connect(&*zookeeper_urls, Duration::from_millis(2500), NoopWatcher) {
            Ok(zk) => zk,
            Err(e) => {
                log::error!("Failed to connect to ZooKeeper: {:?}", e);
                return Err(e.into());
            }
        };

        let zk_arc = Arc::new(zk);

        // Ensure the parent node for service registry exists
        let exists = zk_arc.exists(service_registry_path, false).unwrap();
        if exists.is_none() {
            // Parent node doesn't exist, so create it
            zk_arc.create(service_registry_path, Vec::from(b""), Acl::open_unsafe().clone(), CreateMode::Persistent).unwrap();
        }

        let instances = Arc::new(RwLock::new(Vec::<Instance>::new()));

        let service_path = service_registry_path.to_owned();
        let zk_arc_clone = Arc::clone(&zk_arc);
        let instances_clone = Arc::clone(&instances);
        zk_arc.get_children_w(&service_registry_path, move |_| {
            log::info!("Service path changed: {}", &service_path);
            let zk_arc_clone = Arc::clone(&zk_arc_clone);
            let service_ids = match zk_arc_clone.get_children(&service_path, false) {
                Ok(ids) => ids,
                Err(e) => {
                    log::error!("Error getting children for service path {}: {:?}", service_path, e);
                    return;
                }
            };
            for service_id in service_ids {
                let service_path = format!("{}/{}", &service_path, service_id);
                let zk_arc_clone = Arc::clone(&zk_arc_clone);
                match zk_arc_clone.get_data(&service_path, false) {
                    Ok((data, _)) => {
                        // Assuming the service URL is stored as a String
                        let service_url = String::from_utf8_lossy(&data);
                        instances_clone.write().unwrap().push(Instance {
                            id: service_id.clone(),
                            address: service_url.to_string(),
                        });
                        log::info!("Discovered service: {}, URL: {}", service_id, service_url);
                    }
                    Err(e) => log::error!("Error getting data for service {}: {:?}", service_id, e),
                }
            }
        })?;

        // Register service
        let service_id = Uuid::new_v4().to_string();
        let service_path = format!("{}/{}", service_registry_path, service_id);
        let acls = Acl::open_unsafe().clone();
        zk_arc.create(&service_path, Vec::from(instance_address), acls, CreateMode::Ephemeral).unwrap();

        log::info!("Service registered with ID: {}", service_id);

        // LeaderLatch setup
        let latch = LeaderLatch::new(zk_arc.clone(), service_id.clone(), latch_path.into());
        latch.start()?;

        let zookeeper_config_manager = ZooKeeperConfigManager {
            service_id,
            latch,
            instances: instances.clone(),
        };

        Ok(zookeeper_config_manager)
    }
}

impl ConfigManager for ZooKeeperConfigManager {
    fn is_leader(&self) -> bool {
        self.latch.has_leadership()
    }

    fn get_leader_address(&self) -> Result<String> {
        let instances = self.instances.read().unwrap();
        for instance in instances.iter() {
            log::info!("instance id: {}, leader id: {}", &instance.id, &self.service_id);
            if instance.id == self.service_id {
                return Ok(instance.address.clone());
            }
        }

        Err(anyhow::anyhow!("Leader not found"))
    }

    fn get_follower_addresses(&self) -> Result<Vec<String>> {
        let mut follower_addresses = Vec::<String>::new();
        let instances = self.instances.read().unwrap();
        for instance in instances.iter() {
            if instance.id != self.service_id {
                follower_addresses.push(instance.address.clone());
            }
        }

        Ok(follower_addresses)
    }

    fn get_name(&self) -> String {
        return self.service_id.clone();
    }
}
