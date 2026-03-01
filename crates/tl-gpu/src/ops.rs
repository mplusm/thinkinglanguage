// GpuOps — compute shader dispatch engine

use std::sync::{Arc, OnceLock};
use wgpu;
use wgpu::util::DeviceExt;

use crate::device::GpuDevice;
use crate::tensor::GpuTensor;
use crate::shaders;

/// GPU operations dispatcher with cached pipelines.
pub struct GpuOps {
    device: Arc<GpuDevice>,
    binary_pipeline: OnceLock<wgpu::ComputePipeline>,
    scalar_pipeline: OnceLock<wgpu::ComputePipeline>,
    reduce_pipeline: OnceLock<wgpu::ComputePipeline>,
    matmul_pipeline: OnceLock<wgpu::ComputePipeline>,
    transpose_pipeline: OnceLock<wgpu::ComputePipeline>,
}

impl GpuOps {
    pub fn new(device: Arc<GpuDevice>) -> Self {
        GpuOps {
            device,
            binary_pipeline: OnceLock::new(),
            scalar_pipeline: OnceLock::new(),
            reduce_pipeline: OnceLock::new(),
            matmul_pipeline: OnceLock::new(),
            transpose_pipeline: OnceLock::new(),
        }
    }

    // ── Pipeline builders ──

    fn get_binary_pipeline(&self) -> &wgpu::ComputePipeline {
        self.binary_pipeline.get_or_init(|| {
            self.create_pipeline(shaders::ELEMENTWISE_BINARY, "main")
        })
    }

    fn get_scalar_pipeline(&self) -> &wgpu::ComputePipeline {
        self.scalar_pipeline.get_or_init(|| {
            self.create_pipeline(shaders::SCALAR_MUL, "main")
        })
    }

    fn get_reduce_pipeline(&self) -> &wgpu::ComputePipeline {
        self.reduce_pipeline.get_or_init(|| {
            self.create_pipeline(shaders::REDUCE_SUM, "main")
        })
    }

    fn get_matmul_pipeline(&self) -> &wgpu::ComputePipeline {
        self.matmul_pipeline.get_or_init(|| {
            self.create_pipeline(shaders::MATMUL, "main")
        })
    }

    fn get_transpose_pipeline(&self) -> &wgpu::ComputePipeline {
        self.transpose_pipeline.get_or_init(|| {
            self.create_pipeline(shaders::TRANSPOSE, "main")
        })
    }

    fn create_pipeline(&self, shader_src: &str, entry: &str) -> wgpu::ComputePipeline {
        let module = self.device.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("compute_shader"),
            source: wgpu::ShaderSource::Wgsl(shader_src.into()),
        });
        self.device.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("compute_pipeline"),
            layout: None, // auto-layout
            module: &module,
            entry_point: Some(entry),
            compilation_options: Default::default(),
            cache: None,
        })
    }

    // ── Elementwise binary operations ──

    fn binary_op(&self, a: &GpuTensor, b: &GpuTensor, op: u32) -> Result<GpuTensor, String> {
        if a.numel != b.numel {
            return Err(format!("Shape mismatch: {:?} vs {:?}", a.shape, b.shape));
        }

        let pipeline = self.get_binary_pipeline();
        let dev = &self.device.device;

        let result_buf = dev.create_buffer(&wgpu::BufferDescriptor {
            label: Some("binary_result"),
            size: a.byte_size(),
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        struct Params { len: u32, op: u32 }
        let params = Params { len: a.numel as u32, op };

        let param_buf = dev.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM,
        });

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = dev.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("binary_bg"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry { binding: 0, resource: a.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 1, resource: b.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 2, resource: result_buf.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 3, resource: param_buf.as_entire_binding() },
            ],
        });

        let workgroups = (a.numel as u32 + 255) / 256;
        let mut encoder = dev.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("binary_op") },
        );
        {
            let mut pass = encoder.begin_compute_pass(
                &wgpu::ComputePassDescriptor { label: Some("binary"), timestamp_writes: None },
            );
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(workgroups, 1, 1);
        }
        self.device.queue.submit(std::iter::once(encoder.finish()));

        Ok(GpuTensor {
            buffer: result_buf,
            shape: a.shape.clone(),
            dtype: a.dtype,
            numel: a.numel,
            device: self.device.clone(),
        })
    }

    pub fn add(&self, a: &GpuTensor, b: &GpuTensor) -> Result<GpuTensor, String> {
        self.binary_op(a, b, 0)
    }

    pub fn sub(&self, a: &GpuTensor, b: &GpuTensor) -> Result<GpuTensor, String> {
        self.binary_op(a, b, 1)
    }

    pub fn mul(&self, a: &GpuTensor, b: &GpuTensor) -> Result<GpuTensor, String> {
        self.binary_op(a, b, 2)
    }

    pub fn div(&self, a: &GpuTensor, b: &GpuTensor) -> Result<GpuTensor, String> {
        self.binary_op(a, b, 3)
    }

    // ── Scalar multiplication ──

    pub fn scale(&self, a: &GpuTensor, scalar: f32) -> GpuTensor {
        let pipeline = self.get_scalar_pipeline();
        let dev = &self.device.device;

        let result_buf = dev.create_buffer(&wgpu::BufferDescriptor {
            label: Some("scale_result"),
            size: a.byte_size(),
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        struct Params { len: u32, scalar: f32 }
        let params = Params { len: a.numel as u32, scalar };

        let param_buf = dev.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("scale_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM,
        });

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = dev.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("scale_bg"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry { binding: 0, resource: a.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 1, resource: result_buf.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 2, resource: param_buf.as_entire_binding() },
            ],
        });

        let workgroups = (a.numel as u32 + 255) / 256;
        let mut encoder = dev.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("scale") },
        );
        {
            let mut pass = encoder.begin_compute_pass(
                &wgpu::ComputePassDescriptor { label: Some("scale"), timestamp_writes: None },
            );
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(workgroups, 1, 1);
        }
        self.device.queue.submit(std::iter::once(encoder.finish()));

        GpuTensor {
            buffer: result_buf,
            shape: a.shape.clone(),
            dtype: a.dtype,
            numel: a.numel,
            device: self.device.clone(),
        }
    }

    // ── Reduction operations ──

    pub fn sum(&self, a: &GpuTensor) -> Result<f32, String> {
        let pipeline = self.get_reduce_pipeline();
        let dev = &self.device.device;

        let num_workgroups = (a.numel as u32 + 255) / 256;

        let partial_buf = dev.create_buffer(&wgpu::BufferDescriptor {
            label: Some("reduce_partial"),
            size: (num_workgroups as usize * std::mem::size_of::<f32>()) as u64,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        struct Params { len: u32 }
        let params = Params { len: a.numel as u32 };

        let param_buf = dev.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("reduce_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM,
        });

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = dev.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("reduce_bg"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry { binding: 0, resource: a.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 1, resource: partial_buf.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 2, resource: param_buf.as_entire_binding() },
            ],
        });

        let mut encoder = dev.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("reduce") },
        );
        {
            let mut pass = encoder.begin_compute_pass(
                &wgpu::ComputePassDescriptor { label: Some("reduce"), timestamp_writes: None },
            );
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(num_workgroups, 1, 1);
        }
        self.device.queue.submit(std::iter::once(encoder.finish()));

        // Read partial sums back and finish on CPU
        let partial_tensor = GpuTensor {
            buffer: partial_buf,
            shape: vec![num_workgroups as usize],
            dtype: crate::tensor::DType::F32,
            numel: num_workgroups as usize,
            device: self.device.clone(),
        };
        let partials = partial_tensor.read_f32()?;
        Ok(partials.iter().sum())
    }

    pub fn mean(&self, a: &GpuTensor) -> Result<f32, String> {
        let s = self.sum(a)?;
        Ok(s / a.numel as f32)
    }

    // ── Matrix multiply ──

    pub fn matmul(&self, a: &GpuTensor, b: &GpuTensor) -> Result<GpuTensor, String> {
        if a.shape.len() != 2 || b.shape.len() != 2 {
            return Err("matmul requires 2D tensors".to_string());
        }
        let m = a.shape[0] as u32;
        let k = a.shape[1] as u32;
        let k2 = b.shape[0] as u32;
        let n = b.shape[1] as u32;
        if k != k2 {
            return Err(format!("matmul dimension mismatch: [{m},{k}] x [{k2},{n}]"));
        }

        let pipeline = self.get_matmul_pipeline();
        let dev = &self.device.device;

        let result_numel = (m * n) as usize;
        let result_buf = dev.create_buffer(&wgpu::BufferDescriptor {
            label: Some("matmul_result"),
            size: (result_numel * std::mem::size_of::<f32>()) as u64,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        struct Params { m: u32, k: u32, n: u32, _pad: u32 }
        let params = Params { m, k, n, _pad: 0 };

        let param_buf = dev.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("matmul_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM,
        });

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = dev.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("matmul_bg"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry { binding: 0, resource: a.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 1, resource: b.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 2, resource: result_buf.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 3, resource: param_buf.as_entire_binding() },
            ],
        });

        let wg_x = (n + 15) / 16;
        let wg_y = (m + 15) / 16;
        let mut encoder = dev.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("matmul") },
        );
        {
            let mut pass = encoder.begin_compute_pass(
                &wgpu::ComputePassDescriptor { label: Some("matmul"), timestamp_writes: None },
            );
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(wg_x, wg_y, 1);
        }
        self.device.queue.submit(std::iter::once(encoder.finish()));

        Ok(GpuTensor {
            buffer: result_buf,
            shape: vec![m as usize, n as usize],
            dtype: a.dtype,
            numel: result_numel,
            device: self.device.clone(),
        })
    }

    // ── Transpose ──

    pub fn transpose(&self, a: &GpuTensor) -> Result<GpuTensor, String> {
        if a.shape.len() != 2 {
            return Err("transpose requires a 2D tensor".to_string());
        }
        let rows = a.shape[0] as u32;
        let cols = a.shape[1] as u32;

        let pipeline = self.get_transpose_pipeline();
        let dev = &self.device.device;

        let result_buf = dev.create_buffer(&wgpu::BufferDescriptor {
            label: Some("transpose_result"),
            size: a.byte_size(),
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        #[repr(C)]
        #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
        struct Params { rows: u32, cols: u32 }
        let params = Params { rows, cols };

        let param_buf = dev.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("transpose_params"),
            contents: bytemuck::bytes_of(&params),
            usage: wgpu::BufferUsages::UNIFORM,
        });

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = dev.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("transpose_bg"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry { binding: 0, resource: a.buffer.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 1, resource: result_buf.as_entire_binding() },
                wgpu::BindGroupEntry { binding: 2, resource: param_buf.as_entire_binding() },
            ],
        });

        let wg_x = (cols + 15) / 16;
        let wg_y = (rows + 15) / 16;
        let mut encoder = dev.create_command_encoder(
            &wgpu::CommandEncoderDescriptor { label: Some("transpose") },
        );
        {
            let mut pass = encoder.begin_compute_pass(
                &wgpu::ComputePassDescriptor { label: Some("transpose"), timestamp_writes: None },
            );
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(wg_x, wg_y, 1);
        }
        self.device.queue.submit(std::iter::once(encoder.finish()));

        Ok(GpuTensor {
            buffer: result_buf,
            shape: vec![cols as usize, rows as usize],
            dtype: a.dtype,
            numel: a.numel,
            device: self.device.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tensor::GpuTensor;

    #[test]
    fn test_gpu_add() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        let a = GpuTensor::from_f32(&[1.0, 2.0, 3.0, 4.0], vec![4], device.clone());
        let b = GpuTensor::from_f32(&[10.0, 20.0, 30.0, 40.0], vec![4], device.clone());

        let c = ops.add(&a, &b).unwrap();
        let result = c.read_f32().unwrap();
        assert_eq!(result, vec![11.0, 22.0, 33.0, 44.0]);
    }

    #[test]
    fn test_gpu_sub() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        let a = GpuTensor::from_f32(&[10.0, 20.0, 30.0], vec![3], device.clone());
        let b = GpuTensor::from_f32(&[1.0, 2.0, 3.0], vec![3], device.clone());

        let c = ops.sub(&a, &b).unwrap();
        let result = c.read_f32().unwrap();
        assert_eq!(result, vec![9.0, 18.0, 27.0]);
    }

    #[test]
    fn test_gpu_mul() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        let a = GpuTensor::from_f32(&[2.0, 3.0, 4.0], vec![3], device.clone());
        let b = GpuTensor::from_f32(&[5.0, 6.0, 7.0], vec![3], device.clone());

        let c = ops.mul(&a, &b).unwrap();
        let result = c.read_f32().unwrap();
        assert_eq!(result, vec![10.0, 18.0, 28.0]);
    }

    #[test]
    fn test_gpu_div() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        let a = GpuTensor::from_f32(&[10.0, 20.0, 30.0], vec![3], device.clone());
        let b = GpuTensor::from_f32(&[2.0, 5.0, 10.0], vec![3], device.clone());

        let c = ops.div(&a, &b).unwrap();
        let result = c.read_f32().unwrap();
        assert_eq!(result, vec![5.0, 4.0, 3.0]);
    }

    #[test]
    fn test_gpu_matmul() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        // [2,2] x [2,2]
        let a = GpuTensor::from_f32(&[1.0, 2.0, 3.0, 4.0], vec![2, 2], device.clone());
        let b = GpuTensor::from_f32(&[5.0, 6.0, 7.0, 8.0], vec![2, 2], device.clone());

        let c = ops.matmul(&a, &b).unwrap();
        let result = c.read_f32().unwrap();
        // [1*5+2*7, 1*6+2*8, 3*5+4*7, 3*6+4*8] = [19, 22, 43, 50]
        assert_eq!(result, vec![19.0, 22.0, 43.0, 50.0]);
        assert_eq!(c.shape, vec![2, 2]);
    }

    #[test]
    fn test_gpu_sum() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        let a = GpuTensor::from_f32(&[1.0, 2.0, 3.0, 4.0], vec![4], device.clone());
        let s = ops.sum(&a).unwrap();
        assert!((s - 10.0).abs() < 1e-5);
    }

    #[test]
    fn test_gpu_transpose() {
        let Some(device) = GpuDevice::get() else { return };
        let ops = GpuOps::new(device.clone());

        // [[1,2,3],[4,5,6]] -> [[1,4],[2,5],[3,6]]
        let a = GpuTensor::from_f32(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], vec![2, 3], device.clone());
        let t = ops.transpose(&a).unwrap();
        let result = t.read_f32().unwrap();
        assert_eq!(result, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]);
        assert_eq!(t.shape, vec![3, 2]);
    }
}
