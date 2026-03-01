// GpuTensor — GPU-resident tensor with f32 storage

use std::sync::Arc;
use wgpu;
use tl_ai::TlTensor;
use crate::device::GpuDevice;

/// Data type for GPU tensors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DType {
    F32,
    F64,
}

impl std::fmt::Display for DType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DType::F32 => write!(f, "f32"),
            DType::F64 => write!(f, "f64"),
        }
    }
}

/// A tensor stored on the GPU as an f32 buffer.
pub struct GpuTensor {
    pub buffer: wgpu::Buffer,
    pub shape: Vec<usize>,
    pub dtype: DType,
    pub numel: usize,
    pub device: Arc<GpuDevice>,
}

impl GpuTensor {
    /// Upload a CPU TlTensor (f64) to GPU as f32.
    pub fn from_cpu(tensor: &TlTensor, device: Arc<GpuDevice>) -> Self {
        let data_f32: Vec<f32> = tensor.data.iter().map(|&v| v as f32).collect();
        Self::from_f32(&data_f32, tensor.data.shape().to_vec(), device)
    }

    /// Create a GpuTensor from f32 data.
    pub fn from_f32(data: &[f32], shape: Vec<usize>, device: Arc<GpuDevice>) -> Self {
        let bytes = bytemuck::cast_slice(data);
        let buffer = device.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("gpu_tensor_data"),
            contents: bytes,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
        });

        let numel = data.len();
        GpuTensor { buffer, shape, dtype: DType::F32, numel, device }
    }

    /// Download GPU tensor to CPU as TlTensor (f64).
    pub fn to_cpu(&self) -> Result<TlTensor, String> {
        let f32_data = self.read_f32()?;
        let f64_data: Vec<f64> = f32_data.iter().map(|&v| v as f64).collect();
        let shape = ndarray::IxDyn(&self.shape);
        let array = ndarray::ArrayD::from_shape_vec(shape, f64_data)
            .map_err(|e| format!("Shape mismatch: {e}"))?;
        Ok(TlTensor { data: array, name: None })
    }

    /// Read raw f32 data from the GPU buffer.
    pub fn read_f32(&self) -> Result<Vec<f32>, String> {
        let size = (self.numel * std::mem::size_of::<f32>()) as u64;
        let staging = self.device.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("staging_read"),
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let mut encoder = self.device.device.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("readback") },
        );
        encoder.copy_buffer_to_buffer(&self.buffer, 0, &staging, 0, size);
        self.device.queue.submit(std::iter::once(encoder.finish()));

        let slice = staging.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |result| {
            let _ = tx.send(result);
        });
        self.device.device.poll(wgpu::Maintain::Wait);
        rx.recv()
            .map_err(|e| format!("GPU readback channel error: {e}"))?
            .map_err(|e| format!("GPU readback error: {e}"))?;

        let data = slice.get_mapped_range();
        let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();
        drop(data);
        staging.unmap();

        Ok(result)
    }

    /// Get the total byte size of the buffer.
    pub fn byte_size(&self) -> u64 {
        (self.numel * std::mem::size_of::<f32>()) as u64
    }
}

impl Clone for GpuTensor {
    fn clone(&self) -> Self {
        let size = self.byte_size();
        let new_buffer = self.device.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("gpu_tensor_clone"),
            size,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let mut encoder = self.device.device.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("clone") },
        );
        encoder.copy_buffer_to_buffer(&self.buffer, 0, &new_buffer, 0, size);
        self.device.queue.submit(std::iter::once(encoder.finish()));

        GpuTensor {
            buffer: new_buffer,
            shape: self.shape.clone(),
            dtype: self.dtype,
            numel: self.numel,
            device: self.device.clone(),
        }
    }
}

impl std::fmt::Debug for GpuTensor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GpuTensor(shape={:?}, dtype={}, device={})",
            self.shape, self.dtype, self.device.adapter_name)
    }
}

impl std::fmt::Display for GpuTensor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<gpu_tensor shape={:?} dtype={}>", self.shape, self.dtype)
    }
}

// Bring in the BufferInitDescriptor from wgpu::util
use wgpu::util::DeviceExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_cpu_gpu_cpu() {
        let Some(device) = GpuDevice::get() else { return };

        let cpu_tensor = TlTensor {
            data: ndarray::arr1(&[1.0, 2.0, 3.0, 4.0]).into_dyn(),
            name: None,
        };

        let gpu = GpuTensor::from_cpu(&cpu_tensor, device);
        let back = gpu.to_cpu().unwrap();

        // f32 precision: within 1e-6
        for (a, b) in cpu_tensor.data.iter().zip(back.data.iter()) {
            assert!((a - b).abs() < 1e-6, "mismatch: {a} vs {b}");
        }
    }
}
