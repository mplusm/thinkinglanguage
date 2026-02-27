use std::fmt;

/// A semantic version (wrapper around semver::Version).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    inner: semver::Version,
}

/// A version requirement (wrapper around semver::VersionReq).
#[derive(Debug, Clone)]
pub struct VersionReq {
    inner: semver::VersionReq,
}

impl Version {
    /// Parse a version string like "1.2.3".
    pub fn parse(s: &str) -> Result<Self, String> {
        semver::Version::parse(s)
            .map(|v| Version { inner: v })
            .map_err(|e| format!("Invalid version '{s}': {e}"))
    }

    /// Get the inner semver::Version.
    pub fn as_semver(&self) -> &semver::Version {
        &self.inner
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl VersionReq {
    /// Parse a version requirement like "^1.0", ">=2.0", "1.2".
    pub fn parse(s: &str) -> Result<Self, String> {
        // Try as a requirement first
        if let Ok(req) = semver::VersionReq::parse(s) {
            return Ok(VersionReq { inner: req });
        }
        // If it looks like a bare version, treat as "^version"
        if let Ok(ver) = semver::Version::parse(s) {
            let req = semver::VersionReq::parse(&format!("^{ver}"))
                .map_err(|e| format!("Invalid version req '{s}': {e}"))?;
            return Ok(VersionReq { inner: req });
        }
        Err(format!("Invalid version requirement '{s}'"))
    }

    /// Check if a version matches this requirement.
    pub fn matches(&self, version: &Version) -> bool {
        self.inner.matches(&version.inner)
    }
}

impl fmt::Display for VersionReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_parse_and_display() {
        let v = Version::parse("1.2.3").unwrap();
        assert_eq!(v.to_string(), "1.2.3");

        assert!(Version::parse("not-a-version").is_err());
    }

    #[test]
    fn version_req_matching() {
        let req = VersionReq::parse("^1.0").unwrap();
        assert!(req.matches(&Version::parse("1.0.0").unwrap()));
        assert!(req.matches(&Version::parse("1.9.9").unwrap()));
        assert!(!req.matches(&Version::parse("2.0.0").unwrap()));

        let req2 = VersionReq::parse(">=2.0, <3.0").unwrap();
        assert!(req2.matches(&Version::parse("2.5.0").unwrap()));
        assert!(!req2.matches(&Version::parse("3.0.0").unwrap()));
    }
}
