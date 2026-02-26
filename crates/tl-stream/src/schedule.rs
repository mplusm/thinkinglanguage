// ThinkingLanguage — Schedule and duration parsing

/// Parse a duration string like "5m", "30s", "100ms", "1h", "1d" into milliseconds.
pub fn parse_duration(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".to_string());
    }

    if let Some(n) = s.strip_suffix("ms") {
        n.parse::<u64>()
            .map_err(|_| format!("Invalid milliseconds: {n}"))
    } else if let Some(n) = s.strip_suffix('s') {
        n.parse::<u64>()
            .map(|v| v * 1000)
            .map_err(|_| format!("Invalid seconds: {n}"))
    } else if let Some(n) = s.strip_suffix('m') {
        n.parse::<u64>()
            .map(|v| v * 60 * 1000)
            .map_err(|_| format!("Invalid minutes: {n}"))
    } else if let Some(n) = s.strip_suffix('h') {
        n.parse::<u64>()
            .map(|v| v * 3600 * 1000)
            .map_err(|_| format!("Invalid hours: {n}"))
    } else if let Some(n) = s.strip_suffix('d') {
        n.parse::<u64>()
            .map(|v| v * 86400 * 1000)
            .map_err(|_| format!("Invalid days: {n}"))
    } else {
        // Try plain milliseconds
        s.parse::<u64>()
            .map_err(|_| format!("Invalid duration: {s}. Use suffixes: ms, s, m, h, d"))
    }
}

/// Validate a cron expression (basic 5-field format: min hour dom month dow).
pub fn validate_cron(expr: &str) -> Result<(), String> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!(
            "Cron expression must have 5 fields (min hour dom month dow), got {}",
            fields.len()
        ));
    }

    let ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)];
    let names = ["minute", "hour", "day-of-month", "month", "day-of-week"];

    for (i, field) in fields.iter().enumerate() {
        validate_cron_field(field, ranges[i].0, ranges[i].1, names[i])?;
    }

    Ok(())
}

fn validate_cron_field(field: &str, min: u32, max: u32, name: &str) -> Result<(), String> {
    if field == "*" {
        return Ok(());
    }

    // Handle step syntax: */5, 1-30/2
    if let Some((range_part, step_part)) = field.split_once('/') {
        if range_part != "*" {
            validate_cron_range(range_part, min, max, name)?;
        }
        step_part
            .parse::<u32>()
            .map_err(|_| format!("Invalid step in {name} field: {step_part}"))?;
        return Ok(());
    }

    // Handle comma-separated values
    for part in field.split(',') {
        validate_cron_range(part, min, max, name)?;
    }

    Ok(())
}

fn validate_cron_range(part: &str, min: u32, max: u32, name: &str) -> Result<(), String> {
    if let Some((start_s, end_s)) = part.split_once('-') {
        let start: u32 = start_s
            .parse()
            .map_err(|_| format!("Invalid range start in {name}: {start_s}"))?;
        let end: u32 = end_s
            .parse()
            .map_err(|_| format!("Invalid range end in {name}: {end_s}"))?;
        if start < min || end > max || start > end {
            return Err(format!(
                "Range {start}-{end} out of bounds for {name} ({min}-{max})"
            ));
        }
    } else {
        let val: u32 = part
            .parse()
            .map_err(|_| format!("Invalid value in {name}: {part}"))?;
        if val < min || val > max {
            return Err(format!(
                "Value {val} out of bounds for {name} ({min}-{max})"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_ms() {
        assert_eq!(parse_duration("100ms").unwrap(), 100);
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), 30_000);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), 300_000);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("2h").unwrap(), 7_200_000);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("1d").unwrap(), 86_400_000);
    }

    #[test]
    fn test_parse_duration_plain_ms() {
        assert_eq!(parse_duration("5000").unwrap(), 5000);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn test_validate_cron_basic() {
        assert!(validate_cron("0 0 * * *").is_ok());      // daily at midnight
        assert!(validate_cron("*/5 * * * *").is_ok());     // every 5 minutes
        assert!(validate_cron("0 9 * * 1-5").is_ok());     // weekdays at 9am
        assert!(validate_cron("30 14 1 * *").is_ok());     // 1st of month at 2:30pm
    }

    #[test]
    fn test_validate_cron_invalid() {
        assert!(validate_cron("0 0 *").is_err());           // too few fields
        assert!(validate_cron("60 0 * * *").is_err());      // minute out of range
        assert!(validate_cron("0 25 * * *").is_err());      // hour out of range
    }
}
