// ThinkingLanguage — Data Validation & Fuzzy Matching
// Licensed under MIT OR Apache-2.0
//
// String validation builtins and fuzzy matching functions.

use regex::Regex;
use std::sync::LazyLock;

static EMAIL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$").unwrap()
});

static URL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^https?://[^\s]+$").unwrap()
});

static PHONE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\+]?[(]?[0-9]{1,4}[)]?[-\s\./0-9]*$").unwrap()
});

/// Check if a string is a valid email address.
pub fn is_email(s: &str) -> bool {
    EMAIL_RE.is_match(s)
}

/// Check if a string is a valid HTTP/HTTPS URL.
pub fn is_url(s: &str) -> bool {
    URL_RE.is_match(s)
}

/// Check if a string looks like a phone number.
pub fn is_phone(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Must have at least 7 digits
    let digit_count = trimmed.chars().filter(|c| c.is_ascii_digit()).count();
    digit_count >= 7 && PHONE_RE.is_match(trimmed)
}

/// Check if a value is between low and high (inclusive).
pub fn is_between(val: f64, low: f64, high: f64) -> bool {
    low <= val && val <= high
}

/// Compute the Levenshtein edit distance between two strings.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 { return n; }
    if n == 0 { return m; }

    let mut prev = (0..=n).collect::<Vec<usize>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

/// Compute the American Soundex code for a string.
pub fn soundex(s: &str) -> String {
    let s = s.trim();
    if s.is_empty() {
        return "0000".to_string();
    }

    let chars: Vec<char> = s.chars().collect();
    let first = chars[0].to_ascii_uppercase();
    if !first.is_ascii_alphabetic() {
        return "0000".to_string();
    }

    let code = |c: char| -> Option<char> {
        match c.to_ascii_uppercase() {
            'B' | 'F' | 'P' | 'V' => Some('1'),
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
            'D' | 'T' => Some('3'),
            'L' => Some('4'),
            'M' | 'N' => Some('5'),
            'R' => Some('6'),
            _ => None, // A, E, I, O, U, H, W, Y → ignored
        }
    };

    let mut result = String::with_capacity(4);
    result.push(first);

    let mut last_code = code(first);
    for &c in &chars[1..] {
        if result.len() >= 4 {
            break;
        }
        let c_code = code(c);
        if let Some(cc) = c_code {
            if Some(cc) != last_code {
                result.push(cc);
            }
            last_code = Some(cc);
        } else {
            // H and W don't separate identical codes, but vowels do
            let upper = c.to_ascii_uppercase();
            if upper != 'H' && upper != 'W' {
                last_code = None;
            }
        }
    }

    while result.len() < 4 {
        result.push('0');
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_email_valid() {
        assert!(is_email("user@example.com"));
        assert!(is_email("test.name+tag@domain.co.uk"));
        assert!(is_email("a@b.cc"));
    }

    #[test]
    fn test_is_email_invalid() {
        assert!(!is_email("not-an-email"));
        assert!(!is_email("@missing.com"));
        assert!(!is_email("user@.com"));
    }

    #[test]
    fn test_is_email_edge() {
        assert!(!is_email(""));
        assert!(!is_email(" "));
        assert!(is_email("user123@test-domain.org"));
    }

    #[test]
    fn test_is_url_valid() {
        assert!(is_url("http://example.com"));
        assert!(is_url("https://www.example.com/path?q=1"));
    }

    #[test]
    fn test_is_url_invalid() {
        assert!(!is_url("ftp://files.example.com"));
        assert!(!is_url("not a url"));
        assert!(!is_url(""));
    }

    #[test]
    fn test_is_phone_valid() {
        assert!(is_phone("+1-555-555-5555"));
        assert!(is_phone("(555) 555-5555"));
    }

    #[test]
    fn test_is_phone_invalid() {
        assert!(!is_phone("abc"));
        assert!(!is_phone("123")); // too few digits
        assert!(!is_phone(""));
    }

    #[test]
    fn test_is_between() {
        assert!(is_between(5.0, 1.0, 10.0));
        assert!(is_between(1.0, 1.0, 10.0)); // inclusive low
        assert!(is_between(10.0, 1.0, 10.0)); // inclusive high
        assert!(!is_between(0.0, 1.0, 10.0));
        assert!(!is_between(11.0, 1.0, 10.0));
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("book", "back"), 2);
    }

    #[test]
    fn test_soundex() {
        assert_eq!(soundex("Robert"), "R163");
        assert_eq!(soundex("Rupert"), "R163");
        assert_eq!(soundex("Ashcraft"), "A261");
        assert_eq!(soundex("Tymczak"), "T522");
        assert_eq!(soundex(""), "0000");
    }
}
