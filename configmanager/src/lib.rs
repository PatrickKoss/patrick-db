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
    pub fn new(service_registry_path: &str, leader_election_path: &str, instance_address: &str, zookeeper_urls: &str) -> Result<Self> {
        let zk_arc = connect_to_zookeeper(zookeeper_urls)?;
        ensure_parent_node_exists(&zk_arc, service_registry_path)?;
        let instances = Arc::new(RwLock::new(Vec::<Instance>::new()));
        watch_service_changes(&zk_arc, service_registry_path, &instances)?;
        let service_id = register_service(&zk_arc, service_registry_path, instance_address)?;
        let latch = setup_leader_latch(&zk_arc, &service_id, leader_election_path)?;

        Ok(Self {
            service_id,
            latch,
            instances,
        })
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
        self.service_id.clone()
    }
}

fn connect_to_zookeeper(zookeeper_urls: &str) -> Result<Arc<ZooKeeper>> {
    match ZooKeeper::connect(zookeeper_urls, Duration::from_millis(2500), NoopWatcher) {
        Ok(zk) => Ok(Arc::new(zk)),
        Err(e) => {
            log::error!("Failed to connect to ZooKeeper: {:?}", e);
            Err(e.into())
        }
    }
}

fn ensure_parent_node_exists(zk_arc: &Arc<ZooKeeper>, service_registry_path: &str) -> Result<()> {
    let exists = zk_arc.exists(service_registry_path, false)?;
    if exists.is_none() {
        zk_arc.create(service_registry_path, Vec::from(b""), Acl::open_unsafe().clone(), CreateMode::Persistent)?;
    }

    Ok(())
}

fn watch_service_changes(zk_arc: &Arc<ZooKeeper>, service_registry_path: &str, instances: &Arc<RwLock<Vec<Instance>>>) -> Result<()> {
    let service_path = service_registry_path.to_owned();
    let zk_arc_clone = Arc::clone(zk_arc);
    let instances_clone = Arc::clone(instances);

    zk_arc.get_children_w(service_registry_path, move |_| {
        handle_service_change(&zk_arc_clone, &service_path, &instances_clone)
    })?;

    Ok(())
}

fn handle_service_change(zk_arc: &Arc<ZooKeeper>, service_discovery_path: &str, instances: &Arc<RwLock<Vec<Instance>>>) {
    log::info!("new instance added to service discovery");
    let service_ids = match zk_arc.get_children(service_discovery_path, false) {
        Ok(ids) => ids,
        Err(e) => {
            log::error!("Error getting children for service path {}: {:?}", service_discovery_path, e);
            return;
        }
    };
    for service_id in service_ids {
        handle_instance_id(zk_arc, service_discovery_path, &service_id, instances);
    }
}

fn handle_instance_id(zk_arc: &Arc<ZooKeeper>, service_discovery_path: &str, instance_id: &str, instances: &Arc<RwLock<Vec<Instance>>>) {
    let service_path = format!("{}/{}", &service_discovery_path, instance_id);

    match zk_arc.get_data(&service_path, false) {
        Ok((data, _)) => {
            let instance_address = String::from_utf8_lossy(&data);
            instances.write().unwrap().push(Instance {
                id: instance_id.parse().unwrap(),
                address: instance_address.to_string(),
            });
            log::info!("Discovered instance: {}, Address: {}", instance_id, instance_address);
        }
        Err(e) => log::error!("Error getting data for service {}: {:?}", instance_id, e),
    }
}

fn register_service(zk_arc: &Arc<ZooKeeper>, service_registry_path: &str, instance_address: &str) -> Result<String> {
    let service_id = Uuid::new_v4().to_string();
    let service_path = format!("{}/{}", service_registry_path, service_id);
    zk_arc.create(&service_path, Vec::from(instance_address), Acl::open_unsafe().clone(), CreateMode::Ephemeral)?;
    log::info!("Service registered with ID: {}", service_id);

    Ok(service_id)
}

fn setup_leader_latch(zk_arc: &Arc<ZooKeeper>, service_id: &str, leader_election_path: &str) -> Result<LeaderLatch> {
    let latch = LeaderLatch::new(zk_arc.clone(), service_id.parse()?, leader_election_path.into());
    latch.start()?;

    Ok(latch)
}
