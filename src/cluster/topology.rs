use std::collections::HashMap;
use std::sync::Arc;

use crate::protocol::Value;
use super::slot::HASH_SLOTS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub bus_port: u16,
    pub is_myself: bool,
    pub is_master: bool,
    pub slots: Vec<(u16, u16)>,
}

impl ClusterNode {
    pub fn new(
        id: String,
        ip: String,
        port: u16,
        bus_port: u16,
        is_myself: bool,
        is_master: bool,
    ) -> Self {
        Self {
            id,
            ip,
            port,
            bus_port,
            is_myself,
            is_master,
            slots: Vec::new(),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

#[derive(Clone)]
pub struct ClusterTopology {
    nodes: HashMap<String, Arc<ClusterNode>>,
    slot_to_node: Vec<Option<Arc<ClusterNode>>>,
    myself_id: String,
}

impl ClusterTopology {
    pub fn new(myself: ClusterNode) -> Self {
        let myself_id = myself.id.clone();
        let myself_arc = Arc::new(myself);

        let mut nodes = HashMap::new();
        nodes.insert(myself_id.clone(), myself_arc);

        let slot_to_node = vec![None; HASH_SLOTS as usize];

        Self {
            nodes,
            slot_to_node,
            myself_id,
        }
    }

    pub fn myself(&self) -> Option<Arc<ClusterNode>> {
        self.nodes.get(&self.myself_id).cloned()
    }

    pub fn add_node(&mut self, node: ClusterNode) {
        let id = node.id.clone();
        self.nodes.insert(id, Arc::new(node));
    }

    pub fn add_slots_range(&mut self, node_id: &str, start: u16, end: u16) -> Result<(), String> {
        if start > end || end >= HASH_SLOTS {
            return Err(format!("Invalid slot range {}-{}", start, end));
        }

        let node = self
            .nodes
            .get(node_id)
            .cloned()
            .ok_or_else(|| format!("Unknown node id {}", node_id))?;

        for slot in start..=end {
            self.slot_to_node[slot as usize] = Some(node.clone());
        }

        // Update node's slot list
        if let Some(existing_node) = self.nodes.get_mut(node_id) {
            let mut updated = (**existing_node).clone();
            updated.slots.push((start, end));
            *existing_node = Arc::new(updated);
        }

        Ok(())
    }

    pub fn parse_and_assign_slots(&mut self, node_id: &str, spec: &str) -> Result<(), String> {
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some((start_s, end_s)) = part.split_once('-') {
                let start = start_s.trim().parse::<u16>().map_err(|e| e.to_string())?;
                let end = end_s.trim().parse::<u16>().map_err(|e| e.to_string())?;
                self.add_slots_range(node_id, start, end)?;
            } else {
                let single = part.parse::<u16>().map_err(|e| e.to_string())?;
                self.add_slots_range(node_id, single, single)?;
            }
        }
        Ok(())
    }

    #[inline]
    pub fn owns_slot(&self, slot: u16) -> bool {
        if let Some(owner) = self.get_node_for_slot(slot) {
            owner.id == self.myself_id
        } else {
            false
        }
    }

    #[inline]
    pub fn get_node_for_slot(&self, slot: u16) -> Option<Arc<ClusterNode>> {
        if (slot as usize) < self.slot_to_node.len() {
            self.slot_to_node[slot as usize].clone()
        } else {
            None
        }
    }

    pub fn format_cluster_info(&self) -> String {
        let assigned_count = self.slot_to_node.iter().filter(|s| s.is_some()).count();
        let state = if assigned_count == HASH_SLOTS as usize {
            "ok"
        } else {
            "fail"
        };
        let known_nodes = self.nodes.len();
        let masters = self.nodes.values().filter(|n| n.is_master).count();

        format!(
            "cluster_state:{}\r\ncluster_slots_assigned:{}\r\ncluster_slots_ok:{}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n",
            state, assigned_count, assigned_count, known_nodes, masters
        )
    }

    pub fn format_cluster_nodes(&self) -> String {
        let mut out = String::new();
        for node in self.nodes.values() {
            let flags = if node.is_myself {
                if node.is_master {
                    "myself,master"
                } else {
                    "myself,slave"
                }
            } else if node.is_master {
                "master"
            } else {
                "slave"
            };

            let mut slot_ranges = Vec::new();
            for (start, end) in &node.slots {
                if start == end {
                    slot_ranges.push(format!("{}", start));
                } else {
                    slot_ranges.push(format!("{}-{}", start, end));
                }
            }
            let slots_str = slot_ranges.join(" ");

            out.push_str(&format!(
                "{} {}:{}@{} {} - 0 0 1 connected",
                node.id, node.ip, node.port, node.bus_port, flags
            ));
            if !slots_str.is_empty() {
                out.push(' ');
                out.push_str(&slots_str);
            }
            out.push_str("\r\n");
        }
        out
    }

    pub fn format_cluster_slots(&self) -> Value {
        // Find contiguous ranges of slots mapped to the same master node
        let mut ranges: Vec<(u16, u16, Arc<ClusterNode>)> = Vec::new();
        let mut current: Option<(u16, u16, Arc<ClusterNode>)> = None;

        for (slot_idx, node_opt) in self.slot_to_node.iter().enumerate() {
            let slot = slot_idx as u16;
            match (current.take(), node_opt) {
                (Some((start, _end, node)), Some(cur_node)) if node.id == cur_node.id => {
                    current = Some((start, slot, node));
                }
                (Some((start, end, node)), Some(cur_node)) => {
                    ranges.push((start, end, node));
                    current = Some((slot, slot, cur_node.clone()));
                }
                (Some((start, end, node)), None) => {
                    ranges.push((start, end, node));
                    current = None;
                }
                (None, Some(cur_node)) => {
                    current = Some((slot, slot, cur_node.clone()));
                }
                (None, None) => {}
            }
        }

        if let Some(r) = current {
            ranges.push(r);
        }

        let mut array_elements = Vec::new();
        for (start, end, node) in ranges {
            let master_info = Value::Array(Some(vec![
                Value::string(node.ip.clone()),
                Value::Integer(node.port as i64),
                Value::string(node.id.clone()),
            ]));

            let range_entry = Value::Array(Some(vec![
                Value::Integer(start as i64),
                Value::Integer(end as i64),
                master_info,
            ]));
            array_elements.push(range_entry);
        }

        Value::Array(Some(array_elements))
    }
}
