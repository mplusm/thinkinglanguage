// GpuDevice — singleton device manager for wgpu

use std::sync::{Arc, OnceLock};
use wgpu;

/// A handle to a GPU device and its command queue.
pub struct GpuDevice {
    pub device: wgpu::Device,
    pub queue: wgpu::Queue,
    pub adapter_name: String,
    pub backend: String,
}

static GPU_DEVICE: OnceLock<Option<Arc<GpuDevice>>> = OnceLock::new();

impl GpuDevice {
    /// Get or lazily initialize the GPU device singleton.
    /// Returns None if no GPU is available.
    pub fn get() -> Option<Arc<GpuDevice>> {
        GPU_DEVICE.get_or_init(|| {
            Self::init_device()
        }).clone()
    }

    /// Check if a GPU device is available without full initialization.
    pub fn is_available() -> bool {
        Self::get().is_some()
    }

    fn init_device() -> Option<Arc<GpuDevice>> {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });

        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        }))?;

        let adapter_name = adapter.get_info().name.clone();
        let backend = format!("{:?}", adapter.get_info().backend);

        let (device, queue) = pollster::block_on(adapter.request_device(
            &wgpu::DeviceDescriptor {
                label: Some("tl-gpu"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                ..Default::default()
            },
            None,
        )).ok()?;

        Some(Arc::new(GpuDevice {
            device,
            queue,
            adapter_name,
            backend,
        }))
    }
}

impl std::fmt::Debug for GpuDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GpuDevice({}, {})", self.adapter_name, self.backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_available() {
        // Should not panic regardless of GPU availability
        let _available = GpuDevice::is_available();
    }
}
