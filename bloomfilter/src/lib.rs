use std::collections::HashMap;
use num::{BigInt, Num, ToPrimitive};
use sha2::{Digest, Sha256};

pub struct BloomFilter {
    md5_hasher: Md5Hasher,
    sha_256_hasher: Sha256Hasher,
    bitmap: Vec<bool>,
    size: usize,
    counter: HashMap<usize, i32>
}

struct Md5Hasher {}

impl Md5Hasher {
    fn new() -> Self {
        Self {}
    }
    fn hash(&self, data: impl AsRef<[u8]>) -> BigInt {
        let digest = md5::compute(data);
        let decimal_hex = format!("{:x}", &digest);
        BigInt::from_str_radix(&decimal_hex, 16).unwrap()
    }
}

struct Sha256Hasher {}

impl Sha256Hasher {
    fn new() -> Self {
        Self {}
    }
    fn hash(&self, data: impl AsRef<[u8]>) -> BigInt {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let target = format!("{:x}", result);
        BigInt::from_str_radix(&target, 16).unwrap()
    }
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        let bitmap = vec![false; size];
        BloomFilter { md5_hasher: Md5Hasher::new(), sha_256_hasher: Sha256Hasher::new(), bitmap, size, counter: HashMap::new() }
    }

    pub fn insert(&mut self, data: impl AsRef<[u8]>) {
        let insert_index = (self.md5_hasher.hash(&data) % self.size).to_u32().unwrap() as usize;
        self.bitmap[insert_index] = true;
        self.counter.insert(insert_index, 1 + if self.counter.contains_key(&insert_index) { self.counter[&insert_index] } else { 0 });
        let insert_index = (self.sha_256_hasher.hash(&data) % self.size).to_u32().unwrap() as usize;
        self.bitmap[insert_index] = true;
        self.counter.insert(insert_index, 1 + if self.counter.contains_key(&insert_index) { self.counter[&insert_index] } else { 0 });
    }

    pub fn check(&self, data: impl AsRef<[u8]>) -> bool {
        let insert_index = self.md5_hasher.hash(&data) % self.size;
        if !self.bitmap[insert_index.to_u32().unwrap() as usize] {
            return false;
        }
        let insert_index = self.sha_256_hasher.hash(&data) % self.size;
        if !self.bitmap[insert_index.to_u32().unwrap() as usize] {
            return false;
        }

        true
    }

    pub fn remove(&mut self, data: impl AsRef<[u8]>) {
        let insert_index = (self.md5_hasher.hash(&data) % self.size).to_u32().unwrap() as usize;

        if self.counter.contains_key(&insert_index) {
            let mut el = self.counter[&insert_index];
            el -= 1;
            if el <= 0 {
                el = 0;
                self.bitmap[insert_index] = false;
            }
            *self.counter.get_mut(&insert_index).unwrap() = el;
        }
        let insert_index = (self.sha_256_hasher.hash(&data) % self.size).to_u32().unwrap() as usize;
        if self.counter.contains_key(&insert_index) {
            let mut el = self.counter[&insert_index];
            el -= 1;
            if el <= 0 {
                el = 0;
                self.bitmap[insert_index] = false;
            }
            *self.counter.get_mut(&insert_index).unwrap() = el;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_filter_starts_empty() {
        let filter = BloomFilter::new(100);
        assert_eq!(filter.check("test data"), false);
    }

    #[test]
    fn bloom_filter_adds_and_checks_data() {
        let mut filter = BloomFilter::new(100);
        filter.insert("test data");
        assert_eq!(filter.check("test data"), true);
    }

    #[test]
    fn bloom_filter_does_not_contain_not_inserted_data() {
        let mut filter = BloomFilter::new(100);
        filter.insert("test data");
        assert_eq!(filter.check("other data"), false);
    }

    #[test]
    fn bloom_filter_removes_data() {
        let mut filter = BloomFilter::new(100);
        filter.insert("test data");
        filter.remove("test data");
        assert_eq!(filter.check("test data"), false);
    }

    #[test]
    fn bloom_filter_handles_collision() {
        let mut filter = BloomFilter::new(1); // Force a collision
        filter.insert("test data");
        filter.insert("other data");
        assert_eq!(filter.check("test data"), true);
        assert_eq!(filter.check("other data"), true);
    }
}
