extern crate uuid;
extern crate zookeeper;

use std::{sync::Arc, time::Duration};
use std::sync::{RwLock, RwLockReadGuard};

use anyhow::Result;
use log::info;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zookeeper::{Acl, CreateMode, recipes::leader::LeaderLatch, WatchedEvent, Watcher, ZooKeeper};

pub trait ConfigManager: Send + Send {
    fn is_leader(&self) -> bool;
    fn get_leader_address(&self) -> Result<String>;
    fn get_follower_addresses(&self) -> Result<Vec<String>>;
    fn get_name(&self) -> String;
}

pub trait AddressManager {
    fn get_leader_address(&self) -> Result<String>;
    fn get_follower_addresses(&self) -> Result<Vec<String>>;
}

pub struct ZooKeeperConfigManager {
    service_id: String,
    latch: LeaderLatch,
    instances: Arc<RwLock<Vec<Instance>>>,
}

pub struct ZooKeeperAddressManager {
    instances: Arc<RwLock<Vec<Instance>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    pub address: String,
    pub is_leader: bool,
}

struct NoopWatcher;

impl Watcher for NoopWatcher {
    fn handle(&self, _: WatchedEvent) {}
}

impl ZooKeeperConfigManager {
    pub fn new(service_registry_path: &str, leader_election_path: &str, instance_address: &str, zookeeper_urls: &str) -> Result<Self> {
        let zk_arc = connect_to_zookeeper(zookeeper_urls)?;
        let service_id = Uuid::new_v4().to_string();

        let latch = setup_leader_latch(&zk_arc, &service_id, leader_election_path)?;
        setup_leader_watch(&zk_arc.clone(), &latch.clone(), &service_id, service_registry_path, leader_election_path, instance_address)?;

        ensure_parent_node_exists(&zk_arc, service_registry_path)?;
        let instances = Arc::new(RwLock::new(Vec::<Instance>::new()));
        watch_service_changes(&zk_arc, service_registry_path, &instances)?;
        register_service(&zk_arc, service_id.as_str(), service_registry_path, instance_address, latch.has_leadership())?;
        // trick to init leader and followers, later will be updated by watches
        handle_service_change(&zk_arc, service_registry_path, &instances);

        Ok(Self {
            service_id,
            latch,
            instances,
        })
    }
}

impl ZooKeeperAddressManager {
    pub fn new(service_registry_path: &str, zookeeper_urls: &str) -> Result<Self> {
        let zk_arc = connect_to_zookeeper(zookeeper_urls)?;
        ensure_parent_node_exists(&zk_arc, service_registry_path)?;
        let instances = Arc::new(RwLock::new(Vec::<Instance>::new()));
        watch_service_changes(&zk_arc, service_registry_path, &instances)?;
        handle_service_change(&zk_arc, service_registry_path, &instances);

        Ok(Self {
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

        get_leader_address(instances)
    }

    fn get_follower_addresses(&self) -> Result<Vec<String>> {
        let instances = self.instances.read().unwrap();

        get_follower_addresses(instances)
    }

    fn get_name(&self) -> String {
        self.service_id.clone()
    }
}

impl AddressManager for ZooKeeperAddressManager {
    fn get_leader_address(&self) -> Result<String> {
        let instances = self.instances.read().unwrap();

        get_leader_address(instances)
    }

    fn get_follower_addresses(&self) -> Result<Vec<String>> {
        let instances = self.instances.read().unwrap();

        get_follower_addresses(instances)
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

    // clear instances
    let mut instances_mut = instances.write().unwrap();
    instances_mut.clear();

    for service_id in service_ids {
        handle_instance_id(zk_arc, service_discovery_path, &service_id, &mut instances_mut);
    }

    drop(instances_mut);

    // Reset the watch
    if let Err(e) = watch_service_changes(zk_arc, service_discovery_path, instances) {
        log::error!("Error resetting watch for service path {}: {:?}", service_discovery_path, e);
    }
}

fn handle_instance_id(zk_arc: &Arc<ZooKeeper>, service_discovery_path: &str, instance_id: &str, instances: &mut Vec<Instance>) {
    let service_path = format!("{}/{}", &service_discovery_path, instance_id);

    match zk_arc.get_data(&service_path, false) {
        Ok((data, _)) => {
            let instance: Instance = match bincode::deserialize(&data) {
                Ok(instance) => instance,
                Err(e) => {
                    log::error!("Error deserializing instance data for service {}: {:?}", instance_id, e);
                    return;
                }
            };
            instances.push(instance);
            log::info!("Discovered instance: {}", instance_id);
            log::info!("Discoverd Instances: {:?}", instances);
        }
        Err(e) => log::error!("Error getting data for service {}: {:?}", instance_id, e),
    }
}

fn register_service(zk_arc: &Arc<ZooKeeper>, service_id: &str, service_registry_path: &str, instance_address: &str, is_leader: bool) -> Result<()> {
    let service_path = format!("{}/{}", service_registry_path, service_id);

    let instance = Instance {
        id: service_id.to_owned(),
        is_leader: is_leader,
        address: instance_address.to_string(),
    };
    let buf = bincode::serialize(&instance)?;
    zk_arc.create(&service_path, buf, Acl::open_unsafe().clone(), CreateMode::Ephemeral)?;

    log::info!("Service registered with ID: {}", service_id);

    Ok(())
}

fn update_service(zk_arc: &Arc<ZooKeeper>, service_id: &str, service_registry_path: &str, instance_address: &str, is_leader: bool) -> Result<()> {
    let service_path = format!("{}/{}", service_registry_path, service_id);

    let instance = Instance {
        id: service_id.to_owned(),
        is_leader: is_leader,
        address: instance_address.to_string(),
    };
    let buf = bincode::serialize(&instance)?;
    zk_arc.set_data(&service_path, buf, None)?;

    log::info!("Service updated with ID: {}", service_id);

    Ok(())
}

fn setup_leader_latch(zk_arc: &Arc<ZooKeeper>, service_id: &str, leader_election_path: &str) -> Result<LeaderLatch> {
    let latch = LeaderLatch::new(zk_arc.clone(), service_id.parse()?, leader_election_path.into());
    latch.start()?;

    Ok(latch)
}

fn setup_leader_watch(
    zk_arc: &Arc<ZooKeeper>,
    leader_latch: &LeaderLatch,
    service_id: &str,
    service_registry_path: &str,
    leader_latch_path: &str,
    instance_address: &str,
) -> Result<()> {
    let zk_arc_clone = Arc::clone(zk_arc);
    let leader_latch_clone = leader_latch.clone();
    let service_path = service_registry_path.to_owned();
    let service_id_owned = service_id.to_owned();
    let instance_address_owned = instance_address.to_owned();
    let leader_path = leader_latch_path.to_owned();

    zk_arc.get_children_w(leader_latch_path, move |_| {
        handle_leader_change(&zk_arc_clone, &service_path, &leader_latch_clone, &service_id_owned, &leader_path, &instance_address_owned)
    })?;

    Ok(())
}

fn handle_leader_change(
    zk_arc: &Arc<ZooKeeper>,
    service_discovery_path: &str,
    leader_latch: &LeaderLatch,
    service_id: &str,
    leader_latch_path: &str,
    instance_address: &str,
) {
    log::info!("leader changed. Current service {}, is_leader: {}", service_id, leader_latch.has_leadership());

    match ensure_parent_node_exists(zk_arc, service_discovery_path) {
        Ok(_) => log::info!("service registry path exists"),
        Err(e) => {
            log::error!("error ensuring service registry path exists: {:?}", e);
            return;
        }
    }

    match update_service(zk_arc, service_id, service_discovery_path, instance_address, leader_latch.has_leadership()) {
        Ok(_) => log::info!("service successfully updated"),
        Err(e) => log::error!("error updating service: {:?}", e),
    }

    // Reset the watch
    if let Err(e) = setup_leader_watch(zk_arc, leader_latch, service_id, service_discovery_path, leader_latch_path, instance_address) {
        log::error!("Error resetting watch for service path {}: {:?}", service_discovery_path, e);
    }
}

fn get_leader_address(instances: RwLockReadGuard<Vec<Instance>>) -> Result<String> {
    let leader_instance = instances.iter().find(|&instance| instance.is_leader);

    match leader_instance {
        Some(instance) => Ok(instance.address.clone()),
        None => Err(anyhow::anyhow!("Leader not found")),
    }
}

fn get_follower_addresses(instances: RwLockReadGuard<Vec<Instance>>) -> Result<Vec<String>> {
    let follower_addresses: Vec<String> = instances.iter()
        .filter(|&instance| !instance.is_leader)
        .map(|instance| instance.address.clone())
        .collect();

    Ok(follower_addresses)
}
