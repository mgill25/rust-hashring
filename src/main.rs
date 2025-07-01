use rand::Rng;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub struct HashRing {
    physical_nodes: Vec<u32>,
    virtual_nodes: Vec<u32>,
    virtual_to_physical: HashMap<u32, u32>,
    physical_to_virtual: HashMap<u32, Vec<u32>>,
    server_counter: HashMap<u32, u32>,
}

impl HashRing {
    pub fn new_with_servers(n: u8) -> Self {
        let mut physical_nodes = vec![];
        for _ in 0..n {
            let server_id = Uuid::new_v4().as_u64_pair().0 as u32;
            physical_nodes.push(server_id);
        }
        HashRing {
            physical_nodes,
            virtual_nodes: Vec::new(),
            virtual_to_physical: HashMap::new(),
            physical_to_virtual: HashMap::new(),
            server_counter: HashMap::new(),
        }
    }

    pub fn new() -> Self {
        HashRing {
            physical_nodes: Vec::new(),
            virtual_nodes: Vec::new(),
            virtual_to_physical: HashMap::new(),
            physical_to_virtual: HashMap::new(),
            server_counter: HashMap::new(),
        }
    }

    // Q1: what happens when we initialize the hash ring?
    // A:  the server list is populated with real servers
    // - and we generate corresponding virtual node mappings
    fn create_hash(&self, data: &[u8]) -> u32 {
        let checksum = crc32fast::hash(data);
        checksum
    }

    fn add_server(&mut self) -> u32 {
        let server_id = Uuid::new_v4().as_u64_pair().0 as u32;
        print!("adding server...{:?}\n", server_id);
        self.physical_nodes.push(server_id);
        self.generate_virtual_nodes(server_id);
        self.virtual_nodes.sort();
        server_id 
    }

    fn remove_server(&mut self, server_id: u32) {
        print!("removing server...{}\n", server_id);
        self.physical_nodes = self.physical_nodes
            .iter()
            .filter(|x| {
                *x != &server_id
            })
            .cloned()
            .collect();
        self.remove_virtual_nodes(server_id);
        self.virtual_nodes.sort();
    }

    /// for every physical server we add, we generate many virtual nodes
    /// which get stored in the server_list array.
    fn init_all_servers(&mut self) {
        for s in self.physical_nodes.clone() {
            self.generate_virtual_nodes(s);
        }
        self.virtual_nodes.sort();
    }

    fn generate_vnode_id(&self, server_id: u32, i: i32) -> u32 {
        let vnode_key = format!("s:{}:v:{}", server_id, i);
        let mut virtual_id = self.create_hash(vnode_key.as_bytes());
        // collision detection in case we land on the same virtual node
        while self.virtual_to_physical.contains_key(&virtual_id) {
            virtual_id += 1;
        }
        virtual_id
    }

    // 1. remove all vnodes from self.virtual_nodes
    // 2. remove mappings from self.virtual_to_physical
    // 3. remove inverse mappings from self.physical_to_virtual
    fn remove_virtual_nodes(&mut self, server_id: u32) {
        print!("removing virtual nodes for server {}\n", server_id);
        if let Some(vnodes) = self.physical_to_virtual.remove(&server_id) {
            for vnode in &vnodes {
                self.virtual_to_physical.remove(vnode);
            }
            self.virtual_nodes.retain(|x| !vnodes.contains(x));
        }
    }

    fn generate_virtual_nodes(&mut self, server_id: u32) {
        print!("generating virtual nodes for server {}\n", server_id);
        for i in 0..100 {
            let vnode_id = self.generate_vnode_id(server_id, i);
            self.virtual_nodes.push(vnode_id);
            self.virtual_to_physical.insert(vnode_id, server_id);
            // Get or create the node list for this server
            let node_list = self
                .physical_to_virtual
                .entry(server_id)
                .or_insert_with(Vec::new);
            node_list.push(vnode_id);
        }
    }

    fn pick_server_on_ring(&mut self, data: &[u8]) -> u32 {
        let key_hash = self.create_hash(data);
        let vnode_id = self.virtual_nodes[binary_search_next_greatest(&self.virtual_nodes, key_hash)];
        let server_id = self.virtual_to_physical[&vnode_id];
        self.server_counter.entry(server_id).and_modify(|i| *i += 1).or_insert(1);
        server_id
    }

    fn show_dist(&self) {
        for server_id in self.physical_nodes.iter() {
            if !self.server_counter.contains_key(server_id) {
                print!("\t\tserver[{}] = {}\n", server_id, "NO DATA FOUND");
                continue;
            }
            let picked_count = self.server_counter[server_id];
            print!("\t\tserver[{}] = {}\n", server_id, picked_count);
        }
    }

    fn clear_counters(&mut self) {
        self.server_counter.clear();
    }
}

/**
 * A simple binary search variant.
 */ 
fn binary_search_next_greatest(arr: &Vec<u32>, key: u32) -> usize {
    let mut left = 0;
    let mut right = arr.len() - 1;
    if key >= arr[right] {
        return 0;
    }
    let mut mid;
    while left < right {
        mid = left + (right - left) / 2;
        if arr[mid] <= key {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

// &[u8] is basically random bytes, so let's generate some
// from a pre-defined string set.
fn generate_random_bytes(length: u8) -> Vec<u8> {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as u8)
        .collect::<Vec<u8>>()
}

fn main() {
    // create the hash ring
    let mut h = HashRing::new_with_servers(4);
    h.init_all_servers();

    // generate some test data
    let mut test_data = vec![];
    for _ in 0..1000 {
        test_data.push(generate_random_bytes(10));
    }

    // pick server on the ring
    for d in &test_data {
        h.pick_server_on_ring(&d);
    }
    h.show_dist();

    // Now let's add a new physical server.
    let new_server = h.add_server();
    h.clear_counters();

    // pick again
    for d in &test_data {
        h.pick_server_on_ring(&d);
    }
    h.show_dist();
    
    // remove the server we just added
    // since data is exactly the same + vnode positions and key hashes are deterministic,
    // we will revert to the original distribution.
    h.remove_server(new_server);
    h.clear_counters();
    for d in &test_data {
        h.pick_server_on_ring(&d);
    }
    h.show_dist();
    
}
