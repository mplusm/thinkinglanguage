// ThinkingLanguage — GPU Tensor Support (Phase 32)
// Cross-platform GPU acceleration via wgpu (Vulkan/Metal/DX12/WebGPU).

mod batch;
mod device;
mod ops;
mod shaders;
mod tensor;

pub use batch::BatchInference;
pub use device::GpuDevice;
pub use ops::GpuOps;
pub use tensor::{DType, GpuTensor};
