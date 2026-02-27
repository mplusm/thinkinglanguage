// ThinkingLanguage — Redis Connector
// Licensed under MIT OR Apache-2.0
//
// Stateless Redis key-value operations. Each call opens a connection from the URL.

use redis::{Client, Commands};

/// Validate a Redis URL and return it (for use as connection handle).
pub fn redis_connect(url: &str) -> Result<String, String> {
    let client = Client::open(url)
        .map_err(|e| format!("Redis connection error: {e}"))?;
    // Test the connection
    let mut conn = client.get_connection()
        .map_err(|e| format!("Redis connection error: {e}"))?;
    let _: String = redis::cmd("PING").query(&mut conn)
        .map_err(|e| format!("Redis ping error: {e}"))?;
    Ok(url.to_string())
}

/// Get a value by key. Returns None if key doesn't exist.
pub fn redis_get(url: &str, key: &str) -> Result<Option<String>, String> {
    let client = Client::open(url)
        .map_err(|e| format!("Redis connection error: {e}"))?;
    let mut conn = client.get_connection()
        .map_err(|e| format!("Redis connection error: {e}"))?;
    let result: Option<String> = conn.get(key)
        .map_err(|e| format!("Redis GET error: {e}"))?;
    Ok(result)
}

/// Set a key-value pair.
pub fn redis_set(url: &str, key: &str, value: &str) -> Result<(), String> {
    let client = Client::open(url)
        .map_err(|e| format!("Redis connection error: {e}"))?;
    let mut conn = client.get_connection()
        .map_err(|e| format!("Redis connection error: {e}"))?;
    conn.set::<_, _, ()>(key, value)
        .map_err(|e| format!("Redis SET error: {e}"))?;
    Ok(())
}

/// Delete a key. Returns true if the key was deleted.
pub fn redis_del(url: &str, key: &str) -> Result<bool, String> {
    let client = Client::open(url)
        .map_err(|e| format!("Redis connection error: {e}"))?;
    let mut conn = client.get_connection()
        .map_err(|e| format!("Redis connection error: {e}"))?;
    let deleted: i64 = conn.del(key)
        .map_err(|e| format!("Redis DEL error: {e}"))?;
    Ok(deleted > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running Redis instance
    fn test_redis_connect() {
        let result = redis_connect("redis://127.0.0.1:6379");
        assert!(result.is_ok());
    }

    #[test]
    #[ignore] // Requires a running Redis instance
    fn test_redis_set_get_del() {
        let url = "redis://127.0.0.1:6379";
        redis_set(url, "tl_test_key", "hello").unwrap();
        let val = redis_get(url, "tl_test_key").unwrap();
        assert_eq!(val, Some("hello".to_string()));
        let deleted = redis_del(url, "tl_test_key").unwrap();
        assert!(deleted);
        let val = redis_get(url, "tl_test_key").unwrap();
        assert_eq!(val, None);
    }
}
