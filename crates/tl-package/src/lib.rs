pub mod cache;
pub mod fetch;
pub mod lockfile;
pub mod manifest;
pub mod outdated;
pub mod resolver;
pub mod version;

#[cfg(feature = "registry")]
pub mod registry_client;

pub use cache::PackageCache;
pub use fetch::{fetch_dependency, read_package_manifest};
pub use lockfile::{LockFile, LockedPackage};
pub use manifest::{DepSourceKind, DependencySpec, DetailedDep, Manifest, ProjectConfig};
pub use outdated::OutdatedInfo;
pub use resolver::{DepChange, ResolveReport, VersionConflict};
pub use resolver::{find_package_source, resolve_and_install, resolve_and_install_with_report};
pub use version::{Version, VersionReq};
