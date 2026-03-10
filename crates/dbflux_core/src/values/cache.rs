use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub provider: String,
    pub locator: String,
    pub sub_key: Option<String>,
}

impl CacheKey {
    pub fn new(
        provider: impl Into<String>,
        locator: impl Into<String>,
        sub_key: Option<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            locator: locator.into(),
            sub_key,
        }
    }
}

#[derive(Clone)]
pub enum CachedValue {
    Plain(String),
    Secret(SecretString),
}

struct CacheEntry {
    value: CachedValue,
    fetched_at: Instant,
}

pub struct ValueCache {
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    ttl: Duration,
}

impl ValueCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<CachedValue> {
        let entries = self.entries.read().ok()?;
        let entry = entries.get(key)?;

        if entry.fetched_at.elapsed() > self.ttl {
            return None;
        }

        Some(entry.value.clone())
    }

    pub fn put(&self, key: CacheKey, value: CachedValue) {
        if let Ok(mut entries) = self.entries.write() {
            entries.insert(
                key,
                CacheEntry {
                    value,
                    fetched_at: Instant::now(),
                },
            );
        }
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.clear();
        }
    }

    pub fn invalidate_provider(&self, provider_id: &str) {
        if let Ok(mut entries) = self.entries.write() {
            entries.retain(|k, _| k.provider != provider_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() {
        let cache = ValueCache::new(Duration::from_secs(60));
        let key = CacheKey::new("aws", "my-secret", Some("password".to_string()));
        cache.put(key.clone(), CachedValue::Plain("s3cret".to_string()));
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn expired_entry_returns_none() {
        let cache = ValueCache::new(Duration::from_millis(1));
        let key = CacheKey::new("aws", "my-secret", None);
        cache.put(key.clone(), CachedValue::Plain("val".to_string()));
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn clear_removes_all() {
        let cache = ValueCache::new(Duration::from_secs(60));
        let k1 = CacheKey::new("aws", "s1", None);
        let k2 = CacheKey::new("gcp", "s2", None);
        cache.put(k1.clone(), CachedValue::Plain("a".to_string()));
        cache.put(k2.clone(), CachedValue::Plain("b".to_string()));
        cache.clear();
        assert!(cache.get(&k1).is_none());
        assert!(cache.get(&k2).is_none());
    }

    #[test]
    fn invalidate_provider_removes_only_matching() {
        let cache = ValueCache::new(Duration::from_secs(60));
        let aws_key = CacheKey::new("aws", "s1", None);
        let gcp_key = CacheKey::new("gcp", "s2", None);
        cache.put(aws_key.clone(), CachedValue::Plain("a".to_string()));
        cache.put(gcp_key.clone(), CachedValue::Plain("b".to_string()));
        cache.invalidate_provider("aws");
        assert!(cache.get(&aws_key).is_none());
        assert!(cache.get(&gcp_key).is_some());
    }

    #[test]
    fn ttl_expiry_triggers_cache_miss() {
        let cache = ValueCache::new(Duration::from_millis(50));
        let key = CacheKey::new("aws", "ttl-test", None);

        cache.put(key.clone(), CachedValue::Plain("fresh".to_string()));
        assert!(cache.get(&key).is_some());

        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(&key).is_none());

        cache.put(key.clone(), CachedValue::Plain("refreshed".to_string()));
        let val = cache.get(&key);
        assert!(val.is_some());
        match val.unwrap() {
            CachedValue::Plain(v) => assert_eq!(v, "refreshed"),
            _ => panic!("expected plain value"),
        }
    }

    #[test]
    fn concurrent_access_does_not_deadlock() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ValueCache::new(Duration::from_secs(60)));
        let mut handles = Vec::new();

        for i in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let key = CacheKey::new("provider", &format!("key-{}", i), None);
                cache.put(key.clone(), CachedValue::Plain(format!("val-{}", i)));

                for _ in 0..100 {
                    let _ = cache.get(&key);
                }

                if i % 3 == 0 {
                    cache.invalidate_provider("provider");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread should not panic");
        }
    }

    #[test]
    fn secret_values_cached_and_retrievable() {
        let cache = ValueCache::new(Duration::from_secs(60));
        let key = CacheKey::new("aws", "secret-val", None);

        cache.put(
            key.clone(),
            CachedValue::Secret(secrecy::SecretString::from("hidden".to_string())),
        );

        match cache.get(&key).unwrap() {
            CachedValue::Secret(s) => {
                assert_eq!(secrecy::ExposeSecret::expose_secret(&s), "hidden");
            }
            CachedValue::Plain(_) => panic!("expected Secret variant"),
        }
    }
}
