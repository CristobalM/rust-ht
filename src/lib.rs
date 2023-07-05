use bit_set::BitSet;
use core::panic;
use rand::prelude::*;
use std::collections::HashSet;
use std::fmt::Display;
use std::marker::PhantomData;
use std::option::Option;

pub trait HashableKey: std::cmp::PartialEq + Default + Display {}
pub trait HashValue: Default + Clone + Display {}
pub trait Hasher<K: HashableKey> {
    fn hash(key: &K) -> usize;
}

pub trait HashTable<K: HashableKey, V: HashValue> {
    fn insert(&mut self, key: K, value: V);
    fn has(&self, key: &K) -> bool;
    fn get<'a>(&self, key: &'a K) -> Option<V>;
    fn delete(&mut self, key: &K);
    fn size(&self) -> usize;
    fn capacity(&self) -> usize;
    fn wasted_capacity(&self) -> usize;
}

#[derive(Default, Debug)]
struct KVPair<K: HashableKey, V: HashValue> {
    key: K,
    value: V,
}

type S<K, V> = Option<KVPair<K, V>>;
type VecS<K, V> = Vec<S<K, V>>;

pub struct SimpleHashTable<K: HashableKey, V: HashValue, H: Hasher<K>> {
    data: VecS<K, V>,
    deleted: BitSet,
    slots_used: usize,
    deleted_slots: usize,
    ph_1: PhantomData<H>,
}

impl<K: HashableKey, V: HashValue, H: Hasher<K>> SimpleHashTable<K, V, H> {
    fn simple_resizer(&mut self, next_capacity: usize) -> bool {
        let mut new_data = Vec::<S<K, V>>::with_capacity(next_capacity);
        new_data.resize_with(next_capacity, || None);
        for i in 0..self.data.len() {
            let element = &mut self.data[i];
            if !element.is_none() && !self.deleted.contains(i) {
                let kv = element.as_mut().unwrap();
                let hashed = H::hash(&kv.key);
                for i in 0..next_capacity {
                    let real_pos = (hashed + i) % next_capacity;
                    let element = &mut new_data[real_pos];
                    if element.is_some() {
                        continue;
                    }
                    let owned_kv = std::mem::replace(kv, Default::default());
                    element.replace(owned_kv);
                    break;
                }
            }
        }
        self.deleted.clear();
        self.deleted.reserve_len(next_capacity);
        self.data = new_data;
        self.deleted_slots = 0;

        true
    }

    fn get_pos<'a>(&self, key: &'a K) -> Option<usize> {
        let hashed = H::hash(&key);
        let total_slots = self.deleted_slots + self.slots_used;
        for i in 0..total_slots {
            let real_pos = (hashed + i) % self.data.len();
            let element = &self.data[real_pos];
            if element.is_none() {
                return None;
            }
            if self.deleted.contains(real_pos) {
                continue;
            }
            let unwrapped = element.as_ref().unwrap();
            if unwrapped.key == *key {
                return Some(real_pos);
            }
        }
        None
    }
}

impl<K: HashableKey, V: HashValue, H: Hasher<K>> HashTable<K, V> for SimpleHashTable<K, V, H> {
    fn insert(&mut self, key: K, value: V) {
        {
            let found_pos = self.get_pos(&key);
            if found_pos.is_some() {
                self.data[found_pos.unwrap()] = Some(KVPair {
                    key: key,
                    value: value,
                });
                return;
            }
        }

        let current_capacity = self.data.len();
        let total_used = self.slots_used + self.deleted_slots;
        if total_used >= current_capacity {
            let next_capacity = current_capacity * 2 + 1;
            if !self.simple_resizer(next_capacity) {
                panic!(
                    "couldn't resize from {} to {}",
                    current_capacity, next_capacity
                );
            }
        }
        let total_used = self.slots_used + self.deleted_slots;
        let current_capacity = self.data.len();

        let hashed = H::hash(&key);
        for i in 0..(total_used + 1) {
            let curr_pos = (hashed + i) % current_capacity;
            let element = &self.data[curr_pos];
            let is_deleted = self.deleted.contains(curr_pos);
            if element.is_some() && !is_deleted {
                continue;
            }
            if is_deleted {
                self.deleted.remove(curr_pos);
            }
            self.data[curr_pos] = Some(KVPair {
                key: key,
                value: value,
            });
            self.slots_used += 1;
            break;
        }
    }
    fn delete(&mut self, key: &K) {
        let hashed = H::hash(&key);
        let slots_to_check = self.slots_used + self.deleted_slots;
        for i in 0..slots_to_check {
            let curr = (hashed + i) % self.data.len();
            let element = &mut self.data[curr];
            if element.is_none() {
                return; // not found
            }
            if self.deleted.contains(curr) {
                continue;
            }
            let unwrapped = element.as_mut().unwrap();
            if unwrapped.key == *key {
                // found
                self.deleted.insert(curr);
                self.slots_used -= 1;
                self.deleted_slots += 1;
                element.take();
                return;
            }
            // have to continue checking
        }
    }

    fn has(&self, key: &K) -> bool {
        let hashed = H::hash(&key);
        let total_slots = self.deleted_slots + self.slots_used;
        for i in 0..total_slots {
            let real_pos = (hashed + i) % self.data.len();
            let element = &self.data[real_pos];
            if element.is_none() {
                return false;
            }
            if self.deleted.contains(real_pos) {
                continue;
            }
            let unwrapped = element.as_ref().unwrap();
            if unwrapped.key == *key {
                return true;
            }
        }
        false
    }

    fn get<'a>(&self, key: &'a K) -> Option<V> {
        let hashed = H::hash(&key);
        let total_slots = self.deleted_slots + self.slots_used;
        for i in 0..total_slots {
            let real_pos = (hashed + i) % self.data.len();
            let element = &self.data[real_pos];
            if element.is_none() {
                return None;
            }
            if self.deleted.contains(real_pos) {
                continue;
            }
            let unwrapped = element.as_ref().unwrap();
            if unwrapped.key == *key {
                return Some(unwrapped.value.clone());
            }
        }
        None
    }

    fn size(&self) -> usize {
        self.slots_used
    }

    fn capacity(&self) -> usize {
        self.data.len()
    }

    fn wasted_capacity(&self) -> usize {
        self.deleted_slots
    }
}

pub fn create_simple_hash_table<K: HashableKey, V: HashValue, H: Hasher<K>>(
    capacity: usize,
) -> SimpleHashTable<K, V, H> {
    let mut data = Vec::<S<K, V>>::with_capacity(capacity);
    data.resize_with(capacity, || None);

    SimpleHashTable {
        data: data,
        deleted: BitSet::with_capacity(capacity),
        slots_used: 0,
        deleted_slots: 0,
        ph_1: Default::default(),
    }
}

struct SimpleHasher;
impl Hasher<i64> for SimpleHasher {
    fn hash(key: &i64) -> usize {
        *key as usize
    }
}

impl HashableKey for i64 {}
impl HashValue for i64 {}

type IntegerToIntegerHT = SimpleHashTable<i64, i64, SimpleHasher>;
fn create_integer_to_integer_ht() -> IntegerToIntegerHT {
    return create_simple_hash_table(32);
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut ht: SimpleHashTable<i64, i64, SimpleHasher> = create_integer_to_integer_ht();
        assert_eq!(ht.size(), 0);
        ht.insert(25, 32);
        assert_eq!(ht.size(), 1);
        ht.insert(25, 32);
        assert_eq!(ht.size(), 1);
        ht.insert(25, 32);
        assert_eq!(ht.size(), 1);
        ht.insert(25, 32);
        assert_eq!(ht.size(), 1);
        ht.insert(25, 32);
        assert_eq!(ht.size(), 1);
        let expected = Some(32i64);
        assert_eq!(ht.has(&25), true);
        assert_eq!(ht.size(), 1);
        assert_eq!(ht.get(&25), expected);
        assert_eq!(ht.size(), 1);
        assert_eq!(ht.get(&25), expected);
        ht.insert(26, 32);
        assert_eq!(ht.size(), 2);
        ht.insert(26, 32);
        assert_eq!(ht.size(), 2);
        ht.insert(26, 32);
        assert_eq!(ht.size(), 2);
        ht.insert(26, 32);
        assert_eq!(ht.size(), 2);
        ht.insert(26, 33);
        assert_eq!(ht.size(), 2);
        assert_eq!(*ht.get(&26).as_ref().unwrap(), 33);
        ht.delete(&26);
        assert_eq!(ht.size(), 1);
        assert!(!ht.has(&26))
    }

    #[test]
    fn overflow_test() {
        let mut ht = create_integer_to_integer_ht();
        let sz_check: i64 = 1000000;
        for i in 0..sz_check {
            ht.insert(i, i);
            assert_eq!(ht.size(), (i + 1) as usize);
        }
        assert_eq!(ht.size(), sz_check as usize);
    }

    #[test]
    fn overflow_delete_test() {
        let mut ht = create_integer_to_integer_ht();
        let sz_check: i64 = 1000000;
        for i in 0..sz_check {
            ht.insert(i, i);
            ht.delete(&i);
            assert_eq!(ht.size(), 0 as usize);
        }
    }

    #[test]
    fn overflow_random_test() {
        let mut ht = create_integer_to_integer_ht();
        let sz_check: i64 = 1000;
        let mut hset: HashSet<i64> = HashSet::new();

        for i in 0..sz_check {
            let mut num: i64;
            loop {
                num = rand::thread_rng().gen_range(-1_000_000_000_000..1_000_000_000_000);
                if hset.insert(num) {
                    break;
                }
            }
            ht.insert(num, num);
            for item in hset.iter() {
                assert!(ht.has(&item));
            }
            assert_eq!(ht.size(), (i + 1) as usize);
        }
    }
}
