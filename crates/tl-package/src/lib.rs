pub mod manifest;
pub mod version;
pub mod lockfile;
pub mod cache;
pub mod fetch;
pub mod resolver;

pub use manifest::{Manifest, ProjectConfig, DependencySpec, DetailedDep, DepSourceKind};
pub use version::{Version, VersionReq};
pub use lockfile::{LockFile, LockedPackage};
pub use cache::PackageCache;
pub use fetch::fetch_dependency;
pub use resolver::{resolve_and_install, find_package_source};
