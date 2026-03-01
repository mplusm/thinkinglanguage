# Tensors

TL provides n-dimensional tensor support backed by ndarray, with optional GPU acceleration via wgpu.

## Creating Tensors

Create tensors from nested lists:

```tl
let t = tensor([[1.0, 2.0], [3.0, 4.0]])
```

This creates a 2x2 matrix (2D tensor).

## Operations

Standard tensor operations:

```tl
let a = tensor([[1.0, 2.0], [3.0, 4.0]])
let b = tensor([[5.0, 6.0], [7.0, 8.0]])

let c = matmul(a, b)
let d = a + b
let e = a - b
let f = a * b  // element-wise multiply
```

## Shape and Indexing

Tensors support n-dimensional arrays. Access shape information and individual elements:

```tl
let t = tensor([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
// t has shape [2, 3]
```

## GPU Tensors

**Feature flag:** `gpu`

GPU tensors use wgpu for cross-platform GPU compute:

- **Vulkan** on Linux
- **Metal** on macOS
- **DX12** on Windows
- **WebGPU** in browsers

### Creating GPU Tensors

```tl
let gt = gpu_tensor([[1.0, 2.0], [3.0, 4.0]])
```

### GPU Operations

```tl
let a = gpu_tensor([[1.0, 2.0], [3.0, 4.0]])
let b = gpu_tensor([[5.0, 6.0], [7.0, 8.0]])

let c = gpu_matmul(a, b)
let d = gpu_add(a, b)
let e = gpu_relu(a)
```

### Auto-Dispatch

Binary operators on GPU tensors automatically use GPU compute shaders:

```tl
let a = gpu_tensor([[1.0, 2.0], [3.0, 4.0]])
let b = gpu_tensor([[5.0, 6.0], [7.0, 8.0]])
let c = a + b  // dispatches to GPU automatically
```

### Precision

GPU tensors use f32 storage internally. Values are converted from f64 to f32 on upload and from f32 back to f64 on download. This is a hardware constraint -- most GPUs do not efficiently support f64 compute.

### GpuDevice

The GPU device is a singleton initialized once per process via `OnceLock`. All GPU tensor operations share this device.

### WGSL Compute Shaders

Five built-in WGSL compute shaders handle GPU operations: matmul, add, mul, relu, and sigmoid.

## Python Interop

**Feature flag:** `python`

Convert between TL tensors and numpy arrays via pyo3:

```tl
let np = py_import("numpy")
// TL tensor <-> numpy array conversion
```
