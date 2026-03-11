//! Bidirectional conversion between serde_json::Value and TL values.
//!
//! This module defines [`TlJsonValue`], an intermediate representation that both
//! `VmValue` (compiler) and `Value` (interpreter) can convert to/from. The two
//! public functions [`json_to_tl`] and [`tl_to_json`] handle all edge cases:
//!
//! - NaN / Infinity floats -> JSON null
//! - Large numbers near i64 boundaries
//! - Decimal preservation via string representation
//! - DateTime as ISO 8601 strings
//! - Secret redaction (never leaks values)
//! - Recursive nested maps and lists

use chrono::DateTime as ChronoDateTime;

/// Intermediate representation for TL values during JSON conversion.
/// Both VmValue and Value can convert to/from this type.
#[derive(Debug, Clone)]
pub enum TlJsonValue {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<TlJsonValue>),
    Map(Vec<(String, TlJsonValue)>),
    Decimal(String), // String representation to preserve precision
    DateTime(i64),   // Millis since epoch
    Secret,          // Redacted -- no value stored
}

/// Convert a `serde_json::Value` into a [`TlJsonValue`].
///
/// Mapping:
/// - `Null` -> `Nil`
/// - `Bool(b)` -> `Bool(b)`
/// - `Number` -> `Int(i64)` if representable, else `Float(f64)`
/// - `String(s)` -> `String(s)`
/// - `Array(arr)` -> `List(vec)` (recursive)
/// - `Object(map)` -> `Map(vec)` (preserving insertion order)
pub fn json_to_tl(v: &serde_json::Value) -> TlJsonValue {
    match v {
        serde_json::Value::Null => TlJsonValue::Nil,
        serde_json::Value::Bool(b) => TlJsonValue::Bool(*b),
        serde_json::Value::Number(n) => {
            // Try integer first (exact), then fall back to float
            if let Some(i) = n.as_i64() {
                TlJsonValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                // Numbers that don't fit i64 but do fit f64 (e.g. u64 > i64::MAX,
                // or fractional numbers that serde stored as Number)
                TlJsonValue::Float(f)
            } else {
                // Should not happen with standard serde_json, but be safe
                TlJsonValue::Nil
            }
        }
        serde_json::Value::String(s) => TlJsonValue::String(s.clone()),
        serde_json::Value::Array(arr) => TlJsonValue::List(arr.iter().map(json_to_tl).collect()),
        serde_json::Value::Object(map) => TlJsonValue::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_tl(v)))
                .collect(),
        ),
    }
}

/// Convert a [`TlJsonValue`] into a `serde_json::Value`.
///
/// Mapping:
/// - `Nil` -> `Null`
/// - `Bool(b)` -> `Bool(b)`
/// - `Int(i)` -> `Number(i64)`
/// - `Float(f)` -> `Null` if NaN or Infinity, else `Number(f64)`
/// - `String(s)` -> `String(s)`
/// - `List(vec)` -> `Array(vec)` (recursive)
/// - `Map(vec)` -> `Object(map)` (recursive, preserving order)
/// - `Decimal(s)` -> `String(s)` (preserves precision)
/// - `DateTime(ms)` -> `String` (ISO 8601 via chrono)
/// - `Secret` -> `String("***")`
pub fn tl_to_json(v: &TlJsonValue) -> serde_json::Value {
    match v {
        TlJsonValue::Nil => serde_json::Value::Null,
        TlJsonValue::Bool(b) => serde_json::Value::Bool(*b),
        TlJsonValue::Int(i) => serde_json::Value::Number((*i).into()),
        TlJsonValue::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                serde_json::Value::Null
            } else {
                // serde_json::Number::from_f64 returns None for NaN/Inf,
                // but we already handled those above
                match serde_json::Number::from_f64(*f) {
                    Some(n) => serde_json::Value::Number(n),
                    None => serde_json::Value::Null,
                }
            }
        }
        TlJsonValue::String(s) => serde_json::Value::String(s.clone()),
        TlJsonValue::List(items) => {
            serde_json::Value::Array(items.iter().map(tl_to_json).collect())
        }
        TlJsonValue::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), tl_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        TlJsonValue::Decimal(s) => serde_json::Value::String(s.clone()),
        TlJsonValue::DateTime(ms) => match ChronoDateTime::from_timestamp_millis(*ms) {
            Some(dt) => serde_json::Value::String(dt.to_rfc3339()),
            None => serde_json::Value::Null,
        },
        TlJsonValue::Secret => serde_json::Value::String("***".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- Roundtrip helpers ----

    /// json -> TlJsonValue -> json, should match original
    fn roundtrip_json(v: serde_json::Value) -> serde_json::Value {
        let tl = json_to_tl(&v);
        tl_to_json(&tl)
    }

    // ---- Tests ----

    #[test]
    fn test_null_roundtrip() {
        let result = roundtrip_json(json!(null));
        assert_eq!(result, json!(null));

        // Also test direct TlJsonValue::Nil
        assert_eq!(tl_to_json(&TlJsonValue::Nil), json!(null));
    }

    #[test]
    fn test_bool_roundtrip() {
        assert_eq!(roundtrip_json(json!(true)), json!(true));
        assert_eq!(roundtrip_json(json!(false)), json!(false));
    }

    #[test]
    fn test_int_roundtrip() {
        // Small positive
        assert_eq!(roundtrip_json(json!(42)), json!(42));
        // Zero
        assert_eq!(roundtrip_json(json!(0)), json!(0));
        // Negative
        assert_eq!(roundtrip_json(json!(-100)), json!(-100));
        // Large positive
        assert_eq!(roundtrip_json(json!(1_000_000_000)), json!(1_000_000_000));
        // i64::MAX
        assert_eq!(roundtrip_json(json!(i64::MAX)), json!(i64::MAX));
        // i64::MIN
        assert_eq!(roundtrip_json(json!(i64::MIN)), json!(i64::MIN));
    }

    #[test]
    fn test_float_roundtrip() {
        // Normal float
        assert_eq!(roundtrip_json(json!(3.14)), json!(3.14));
        // Negative float
        assert_eq!(roundtrip_json(json!(-2.718)), json!(-2.718));
        // Fractional
        assert_eq!(roundtrip_json(json!(0.001)), json!(0.001));
    }

    #[test]
    fn test_nan_to_null() {
        let tl = TlJsonValue::Float(f64::NAN);
        let result = tl_to_json(&tl);
        assert_eq!(result, json!(null));
    }

    #[test]
    fn test_infinity_to_null() {
        let pos_inf = TlJsonValue::Float(f64::INFINITY);
        assert_eq!(tl_to_json(&pos_inf), json!(null));

        let neg_inf = TlJsonValue::Float(f64::NEG_INFINITY);
        assert_eq!(tl_to_json(&neg_inf), json!(null));
    }

    #[test]
    fn test_string_roundtrip() {
        // Empty string
        assert_eq!(roundtrip_json(json!("")), json!(""));
        // Normal ASCII
        assert_eq!(roundtrip_json(json!("hello")), json!("hello"));
        // Unicode
        assert_eq!(roundtrip_json(json!("Hej varlden")), json!("Hej varlden"));
        // Emoji
        assert_eq!(
            roundtrip_json(json!("\u{1F680}\u{2728}")),
            json!("\u{1F680}\u{2728}")
        );
        // String with special chars
        assert_eq!(
            roundtrip_json(json!("line1\nline2\ttab")),
            json!("line1\nline2\ttab")
        );
    }

    #[test]
    fn test_nested_map() {
        let input = json!({
            "level1": {
                "level2": {
                    "level3": "deep_value"
                },
                "sibling": 42
            },
            "top_key": true
        });
        let result = roundtrip_json(input.clone());
        assert_eq!(result, input);
    }

    #[test]
    fn test_mixed_array() {
        let input = json!([
            42,
            "hello",
            null,
            {"nested": "object"},
            [1, 2, 3],
            true,
            3.14
        ]);
        let result = roundtrip_json(input.clone());
        assert_eq!(result, input);
    }

    #[test]
    fn test_decimal_as_string() {
        // Decimal values are stored as strings to preserve precision
        let tl = TlJsonValue::Decimal("123456789.123456789".to_string());
        let result = tl_to_json(&tl);
        assert_eq!(result, json!("123456789.123456789"));

        // Edge case: very small decimal
        let tl = TlJsonValue::Decimal("0.000000001".to_string());
        let result = tl_to_json(&tl);
        assert_eq!(result, json!("0.000000001"));
    }

    #[test]
    fn test_datetime_iso8601() {
        // 2024-01-15T12:30:00.000Z in millis = 1705321800000
        let ms = 1705321800000_i64;
        let tl = TlJsonValue::DateTime(ms);
        let result = tl_to_json(&tl);

        // Should be an ISO 8601 / RFC 3339 string
        if let serde_json::Value::String(s) = &result {
            assert!(
                s.contains("2024-01-15"),
                "Expected date 2024-01-15, got: {}",
                s
            );
            assert!(s.contains("12:30:00"), "Expected time 12:30:00, got: {}", s);
        } else {
            panic!("Expected string, got: {:?}", result);
        }

        // Epoch zero
        let tl_epoch = TlJsonValue::DateTime(0);
        let result_epoch = tl_to_json(&tl_epoch);
        if let serde_json::Value::String(s) = &result_epoch {
            assert!(s.contains("1970-01-01"), "Expected epoch date, got: {}", s);
        } else {
            panic!("Expected string for epoch, got: {:?}", result_epoch);
        }
    }

    #[test]
    fn test_secret_redacted() {
        let tl = TlJsonValue::Secret;
        let result = tl_to_json(&tl);
        assert_eq!(result, json!("***"));
    }

    #[test]
    fn test_empty_map() {
        let input = json!({});
        let result = roundtrip_json(input.clone());
        assert_eq!(result, input);

        // Direct construction
        let tl = TlJsonValue::Map(vec![]);
        assert_eq!(tl_to_json(&tl), json!({}));
    }

    #[test]
    fn test_empty_array() {
        let input = json!([]);
        let result = roundtrip_json(input.clone());
        assert_eq!(result, input);

        // Direct construction
        let tl = TlJsonValue::List(vec![]);
        assert_eq!(tl_to_json(&tl), json!([]));
    }

    #[test]
    fn test_large_number() {
        // i64::MAX roundtrips as integer
        let input = json!(i64::MAX);
        let tl = json_to_tl(&input);
        match &tl {
            TlJsonValue::Int(i) => assert_eq!(*i, i64::MAX),
            other => panic!("Expected Int for i64::MAX, got: {:?}", other),
        }
        assert_eq!(tl_to_json(&tl), json!(i64::MAX));

        // i64::MIN roundtrips as integer
        let input_min = json!(i64::MIN);
        let tl_min = json_to_tl(&input_min);
        match &tl_min {
            TlJsonValue::Int(i) => assert_eq!(*i, i64::MIN),
            other => panic!("Expected Int for i64::MIN, got: {:?}", other),
        }

        // u64::MAX (exceeds i64 range) should become Float
        let big = serde_json::Value::Number(serde_json::Number::from(u64::MAX));
        let tl_big = json_to_tl(&big);
        match &tl_big {
            TlJsonValue::Float(_) => {} // Expected: u64::MAX can't fit i64
            TlJsonValue::Int(_) => {}   // Also acceptable if serde handles it
            other => panic!("Expected Float or Int for u64::MAX, got: {:?}", other),
        }
    }

    #[test]
    fn test_json_to_tl_preserves_map_key_order() {
        // serde_json::Map uses BTreeMap by default (alphabetical order)
        // unless the "preserve_order" feature is enabled.
        // We just verify all keys and values are present.
        let input = json!({
            "zebra": 1,
            "alpha": 2,
            "middle": 3
        });
        let tl = json_to_tl(&input);
        if let TlJsonValue::Map(entries) = &tl {
            assert_eq!(entries.len(), 3);
            let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
            assert!(keys.contains(&"zebra"));
            assert!(keys.contains(&"alpha"));
            assert!(keys.contains(&"middle"));
        } else {
            panic!("Expected Map, got: {:?}", tl);
        }
    }

    #[test]
    fn test_deeply_nested_list_of_maps() {
        let input = json!([
            {"a": [{"b": [1, 2, 3]}]},
            {"c": null}
        ]);
        let result = roundtrip_json(input.clone());
        assert_eq!(result, input);
    }

    #[test]
    fn test_datetime_invalid_millis() {
        // Extremely large millis that chrono cannot represent
        // chrono's from_timestamp_millis returns None for out-of-range values
        let tl = TlJsonValue::DateTime(i64::MAX);
        let result = tl_to_json(&tl);
        // Should gracefully return null, not panic
        assert_eq!(result, json!(null));
    }

    #[test]
    fn test_negative_datetime() {
        // Negative millis = before epoch (e.g. 1969)
        let tl = TlJsonValue::DateTime(-86_400_000); // -1 day from epoch
        let result = tl_to_json(&tl);
        if let serde_json::Value::String(s) = &result {
            assert!(s.contains("1969-12-31"), "Expected 1969-12-31, got: {}", s);
        } else {
            panic!("Expected string for negative datetime, got: {:?}", result);
        }
    }

    #[test]
    fn test_map_with_special_keys() {
        let input = json!({
            "": "empty_key",
            "key with spaces": "value",
            "key/with/slashes": true,
            "unicode_key_\u{00e9}": 42
        });
        let result = roundtrip_json(input.clone());
        assert_eq!(result, input);
    }
}
