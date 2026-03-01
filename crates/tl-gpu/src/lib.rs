// ThinkingLanguage — GPU Tensor Support (Phase 32)
// Cross-platform GPU acceleration via wgpu (Vulkan/Metal/DX12/WebGPU).

mod device;
mod tensor;
mod shaders;
mod ops;
mod batch;

pub use device::GpuDevice;
pub use tensor::{GpuTensor, DType};
pub use ops::GpuOps;
pub use batch::BatchInference;
