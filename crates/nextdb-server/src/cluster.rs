#![cfg_attr(not(feature = "cluster"), allow(dead_code))]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterNode {
    pub id: String,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterShard {
    pub shard: usize,
    pub epoch: u64,
    pub owner: String,
    pub replicas: Vec<String>,
    pub role: ShardRole,
    pub owner_url: Option<String>,
    pub replica_urls: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ShardRole {
    Owner,
    Replica,
    Unassigned,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTopology {
    pub node_id: String,
    pub node_url: Option<String>,
    pub shard_count: usize,
    pub enforce_ownership: bool,
    pub nodes: Vec<ClusterNode>,
    pub shards: Vec<ClusterShard>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardRoute {
    pub key: String,
    pub shard: usize,
    pub epoch: u64,
    pub owner: String,
    pub owner_url: Option<String>,
    pub replicas: Vec<String>,
    pub replica_urls: Vec<String>,
    pub local_role: ShardRole,
    pub local_accepts_writes: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterShardOverride {
    pub owner: Option<String>,
    pub epoch: Option<u64>,
    pub replicas: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    node_id: String,
    node_url: Option<String>,
    enforce_ownership: bool,
    nodes: BTreeMap<String, ClusterNode>,
    owners: Vec<String>,
    epochs: Vec<u64>,
    replicas: Vec<Vec<String>>,
}

impl ClusterConfig {
    pub fn from_env(shard_count: usize) -> Self {
        #[cfg(not(feature = "cluster"))]
        {
            Self::single_node(shard_count)
        }

        #[cfg(feature = "cluster")]
        {
            let node_id = std::env::var("NEXTDB_NODE_ID")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "local".to_string());
            let node_url = std::env::var("NEXTDB_NODE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty());
            let enforce_ownership =
                parse_bool_env("NEXTDB_ENFORCE_SHARD_OWNERSHIP").unwrap_or(false);

            let mut nodes = parse_nodes_env("NEXTDB_CLUSTER_NODES");
            nodes.entry(node_id.clone()).or_insert_with(|| ClusterNode {
                id: node_id.clone(),
                url: node_url.clone(),
            });

            let mut owners = vec![node_id.clone(); shard_count];
            let mut epochs = vec![1_u64; shard_count];
            if let Ok(value) = std::env::var("NEXTDB_SHARD_OWNERS") {
                apply_shard_assignments(&value, shard_count, |shard, node| {
                    owners[shard] = node.to_string();
                    nodes
                        .entry(node.to_string())
                        .or_insert_with(|| ClusterNode {
                            id: node.to_string(),
                            url: None,
                        });
                });
            }
            if let Ok(value) = std::env::var("NEXTDB_SHARD_EPOCHS") {
                apply_shard_assignments(&value, shard_count, |shard, epoch| {
                    if let Ok(epoch) = epoch.trim().parse::<u64>() {
                        epochs[shard] = epoch.max(1);
                    }
                });
            }

            let mut replicas = vec![Vec::new(); shard_count];
            if let Ok(value) = std::env::var("NEXTDB_SHARD_REPLICAS") {
                apply_replica_assignments(&value, shard_count, |shard, replica_nodes| {
                    replicas[shard] = replica_nodes
                        .iter()
                        .filter(|replica| !replica.is_empty())
                        .map(|replica| {
                            nodes
                                .entry(replica.to_string())
                                .or_insert_with(|| ClusterNode {
                                    id: replica.to_string(),
                                    url: None,
                                });
                            replica.to_string()
                        })
                        .collect();
                });
            }

            Self {
                node_id,
                node_url,
                enforce_ownership,
                nodes,
                owners,
                epochs,
                replicas,
            }
        }
    }

    #[cfg(any(not(feature = "cluster"), test))]
    pub fn single_node(shard_count: usize) -> Self {
        let node_id = "local".to_string();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node_id.clone(),
            ClusterNode {
                id: node_id.clone(),
                url: None,
            },
        );
        Self {
            node_id: node_id.clone(),
            node_url: None,
            enforce_ownership: false,
            nodes,
            owners: vec![node_id; shard_count],
            epochs: vec![1; shard_count],
            replicas: vec![Vec::new(); shard_count],
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn enforce_ownership(&self) -> bool {
        self.enforce_ownership
    }

    pub fn shard_count(&self) -> usize {
        self.owners.len()
    }

    pub fn route_for_key_with_overrides(
        &self,
        key: String,
        shard: usize,
        overrides: &BTreeMap<usize, ClusterShardOverride>,
    ) -> ShardRoute {
        let owner = self.owner_for_shard(shard).to_string();
        let owner = overrides
            .get(&shard)
            .and_then(|override_value| override_value.owner.clone())
            .unwrap_or(owner);
        let replicas = self.replicas_for_shard_with_overrides(shard, overrides);
        let local_role = self.role_for_shard_with_overrides(shard, overrides);
        ShardRoute {
            key,
            shard,
            epoch: self.epoch_for_shard_with_overrides(shard, overrides),
            owner: owner.clone(),
            owner_url: self.node_url_for(&owner),
            replica_urls: replicas
                .iter()
                .filter_map(|replica| self.node_url_for(replica))
                .collect(),
            replicas,
            local_accepts_writes: !self.enforce_ownership || local_role == ShardRole::Owner,
            local_role,
        }
    }

    pub fn owner_for_shard(&self, shard: usize) -> &str {
        self.owners
            .get(shard)
            .map(String::as_str)
            .unwrap_or(self.node_id.as_str())
    }

    pub fn owner_for_shard_with_overrides(
        &self,
        shard: usize,
        overrides: &BTreeMap<usize, ClusterShardOverride>,
    ) -> String {
        #[cfg(not(feature = "cluster"))]
        {
            let _ = overrides;
            self.owner_for_shard(shard).to_string()
        }

        #[cfg(feature = "cluster")]
        {
            overrides
                .get(&shard)
                .and_then(|override_value| override_value.owner.clone())
                .unwrap_or_else(|| self.owner_for_shard(shard).to_string())
        }
    }

    pub fn epoch_for_shard(&self, shard: usize) -> u64 {
        self.epochs.get(shard).copied().unwrap_or(1)
    }

    pub fn epoch_for_shard_with_overrides(
        &self,
        shard: usize,
        overrides: &BTreeMap<usize, ClusterShardOverride>,
    ) -> u64 {
        #[cfg(not(feature = "cluster"))]
        {
            let _ = overrides;
            self.epoch_for_shard(shard)
        }

        #[cfg(feature = "cluster")]
        {
            overrides
                .get(&shard)
                .and_then(|override_value| override_value.epoch)
                .unwrap_or_else(|| self.epoch_for_shard(shard))
        }
    }

    pub fn replicas_for_shard(&self, shard: usize) -> &[String] {
        self.replicas.get(shard).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn replicas_for_shard_with_overrides(
        &self,
        shard: usize,
        overrides: &BTreeMap<usize, ClusterShardOverride>,
    ) -> Vec<String> {
        #[cfg(not(feature = "cluster"))]
        {
            let _ = overrides;
            self.replicas_for_shard(shard).to_vec()
        }

        #[cfg(feature = "cluster")]
        {
            overrides
                .get(&shard)
                .and_then(|override_value| override_value.replicas.clone())
                .unwrap_or_else(|| self.replicas_for_shard(shard).to_vec())
        }
    }

    pub fn role_for_shard_with_overrides(
        &self,
        shard: usize,
        overrides: &BTreeMap<usize, ClusterShardOverride>,
    ) -> ShardRole {
        if self.owner_for_shard_with_overrides(shard, overrides) == self.node_id {
            return ShardRole::Owner;
        }
        if self
            .replicas_for_shard_with_overrides(shard, overrides)
            .iter()
            .any(|replica| replica == &self.node_id)
        {
            return ShardRole::Replica;
        }
        ShardRole::Unassigned
    }

    pub fn node_url_for(&self, node_id: &str) -> Option<String> {
        self.nodes.get(node_id).and_then(|node| node.url.clone())
    }

    pub fn topology_with_overrides(
        &self,
        overrides: &BTreeMap<usize, ClusterShardOverride>,
    ) -> ClusterTopology {
        ClusterTopology {
            node_id: self.node_id.clone(),
            node_url: self.node_url.clone(),
            shard_count: self.shard_count(),
            enforce_ownership: self.enforce_ownership,
            nodes: self.nodes.values().cloned().collect(),
            shards: (0..self.shard_count())
                .map(|shard| {
                    let owner = self.owner_for_shard_with_overrides(shard, overrides);
                    let replicas = self.replicas_for_shard_with_overrides(shard, overrides);
                    ClusterShard {
                        shard,
                        epoch: self.epoch_for_shard_with_overrides(shard, overrides),
                        owner: owner.clone(),
                        replicas: replicas.clone(),
                        role: self.role_for_shard_with_overrides(shard, overrides),
                        owner_url: self.node_url_for(&owner),
                        replica_urls: replicas
                            .iter()
                            .filter_map(|replica| self.node_url_for(replica))
                            .collect(),
                    }
                })
                .collect(),
        }
    }
}

pub fn cluster_enabled() -> bool {
    cfg!(feature = "cluster")
}

#[cfg(feature = "cluster")]
fn parse_nodes_env(name: &str) -> BTreeMap<String, ClusterNode> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|entry| {
            let (id, url) = entry.split_once('=')?;
            let id = id.trim();
            if id.is_empty() {
                return None;
            }
            Some((
                id.to_string(),
                ClusterNode {
                    id: id.to_string(),
                    url: Some(url.trim().to_string()).filter(|value| !value.is_empty()),
                },
            ))
        })
        .collect()
}

#[cfg(feature = "cluster")]
fn apply_shard_assignments(value: &str, shard_count: usize, mut assign: impl FnMut(usize, &str)) {
    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if let Some((range, node)) = entry.split_once('=') {
            for shard in parse_shard_range(range, shard_count) {
                assign(shard, node.trim());
            }
        } else if let Some((node, range)) = entry.split_once(':') {
            for shard in parse_shard_range(range, shard_count) {
                assign(shard, node.trim());
            }
        }
    }
}

#[cfg(feature = "cluster")]
fn apply_replica_assignments(
    value: &str,
    shard_count: usize,
    mut assign: impl FnMut(usize, Vec<&str>),
) {
    for entry in value
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((range, nodes)) = entry.split_once('=') else {
            continue;
        };
        let replica_nodes: Vec<_> = nodes
            .split('|')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .collect();
        for shard in parse_shard_range(range, shard_count) {
            assign(shard, replica_nodes.clone());
        }
    }
}

#[cfg(feature = "cluster")]
fn parse_shard_range(value: &str, shard_count: usize) -> Vec<usize> {
    let trimmed = value.trim();
    if let Some((start, end)) = trimmed.split_once('-') {
        let start = start.trim().parse::<usize>().ok();
        let end = end.trim().parse::<usize>().ok();
        match (start, end) {
            (Some(start), Some(end)) if start <= end => (start..=end)
                .filter(|shard| *shard < shard_count)
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        }
    } else {
        trimmed
            .parse::<usize>()
            .ok()
            .filter(|shard| *shard < shard_count)
            .into_iter()
            .collect()
    }
}

#[cfg(feature = "cluster")]
fn parse_bool_env(name: &str) -> Option<bool> {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "cluster"))]
    #[test]
    fn default_build_ignores_cluster_overrides() {
        let config = ClusterConfig::from_env(2);
        let overrides = BTreeMap::from([(
            0,
            ClusterShardOverride {
                owner: Some("remote".to_string()),
                epoch: Some(99),
                replicas: Some(vec!["replica-a".to_string()]),
            },
        )]);

        assert!(!cluster_enabled());
        assert_eq!(config.node_id(), "local");
        assert_eq!(
            config.owner_for_shard_with_overrides(0, &overrides),
            "local"
        );
        assert_eq!(config.epoch_for_shard_with_overrides(0, &overrides), 1);
        assert!(
            config
                .replicas_for_shard_with_overrides(0, &overrides)
                .is_empty()
        );
        assert_eq!(
            config.role_for_shard_with_overrides(0, &overrides),
            ShardRole::Owner
        );

        let topology = config.topology_with_overrides(&overrides);
        assert!(!topology.enforce_ownership);
        assert_eq!(topology.shards[0].owner, "local");
        assert!(topology.shards[0].replicas.is_empty());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn cluster_feature_applies_overrides() {
        let config = ClusterConfig::single_node(1);
        let overrides = BTreeMap::from([(
            0,
            ClusterShardOverride {
                owner: Some("remote".to_string()),
                epoch: Some(99),
                replicas: Some(vec!["local".to_string()]),
            },
        )]);

        assert!(cluster_enabled());
        assert_eq!(
            config.owner_for_shard_with_overrides(0, &overrides),
            "remote"
        );
        assert_eq!(config.epoch_for_shard_with_overrides(0, &overrides), 99);
        assert_eq!(
            config.replicas_for_shard_with_overrides(0, &overrides),
            vec!["local".to_string()]
        );
        assert_eq!(
            config.role_for_shard_with_overrides(0, &overrides),
            ShardRole::Replica
        );
    }
}
