pub mod manifest;
pub mod version;
pub mod lockfile;
pub mod cache;
pub mod fetch;
pub mod resolver;
pub mod outdated;

#[cfg(feature = "registry")]
pub mod registry_client;

pub use manifest::{Manifest, ProjectConfig, DependencySpec, DetailedDep, DepSourceKind};
pub use version::{Version, VersionReq};
pub use lockfile::{LockFile, LockedPackage};
pub use cache::PackageCache;
pub use fetch::{fetch_dependency, read_package_manifest};
pub use resolver::{resolve_and_install, resolve_and_install_with_report, find_package_source};
pub use resolver::{ResolveReport, DepChange, VersionConflict};
pub use outdated::OutdatedInfo;
