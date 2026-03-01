// WGSL compute shaders for GPU tensor operations

/// Elementwise binary operation: add(0), sub(1), mul(2), div(3)
pub const ELEMENTWISE_BINARY: &str = r#"
@group(0) @binding(0) var<storage, read> a: array<f32>;
@group(0) @binding(1) var<storage, read> b: array<f32>;
@group(0) @binding(2) var<storage, read_write> result: array<f32>;

struct Params {
    len: u32,
    op: u32,
}
@group(0) @binding(3) var<uniform> params: Params;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let idx = gid.x;
    if (idx >= params.len) {
        return;
    }
    let va = a[idx];
    let vb = b[idx];
    switch (params.op) {
        case 0u: { result[idx] = va + vb; }
        case 1u: { result[idx] = va - vb; }
        case 2u: { result[idx] = va * vb; }
        case 3u: { result[idx] = va / vb; }
        default: { result[idx] = 0.0; }
    }
}
"#;

/// Scalar multiplication: scale all elements by a scalar
pub const SCALAR_MUL: &str = r#"
@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read_write> result: array<f32>;

struct Params {
    len: u32,
    scalar: f32,
}
@group(0) @binding(2) var<uniform> params: Params;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let idx = gid.x;
    if (idx >= params.len) {
        return;
    }
    result[idx] = input[idx] * params.scalar;
}
"#;

/// Parallel sum reduction using shared memory
pub const REDUCE_SUM: &str = r#"
@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read_write> result: array<f32>;

struct Params {
    len: u32,
}
@group(0) @binding(2) var<uniform> params: Params;

var<workgroup> shared: array<f32, 256>;

@compute @workgroup_size(256)
fn main(
    @builtin(local_invocation_id) lid: vec3<u32>,
    @builtin(global_invocation_id) gid: vec3<u32>,
    @builtin(workgroup_id) wid: vec3<u32>,
) {
    let idx = gid.x;
    if (idx < params.len) {
        shared[lid.x] = input[idx];
    } else {
        shared[lid.x] = 0.0;
    }
    workgroupBarrier();

    // Tree reduction
    var stride: u32 = 128u;
    loop {
        if (stride == 0u) {
            break;
        }
        if (lid.x < stride) {
            shared[lid.x] = shared[lid.x] + shared[lid.x + stride];
        }
        workgroupBarrier();
        stride = stride >> 1u;
    }

    if (lid.x == 0u) {
        result[wid.x] = shared[0];
    }
}
"#;

/// Naive 2D matrix multiplication: C = A * B
/// A is [M, K], B is [K, N], C is [M, N]
pub const MATMUL: &str = r#"
@group(0) @binding(0) var<storage, read> a: array<f32>;
@group(0) @binding(1) var<storage, read> b: array<f32>;
@group(0) @binding(2) var<storage, read_write> result: array<f32>;

struct Params {
    m: u32,
    k: u32,
    n: u32,
}
@group(0) @binding(3) var<uniform> params: Params;

@compute @workgroup_size(16, 16)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let row = gid.y;
    let col = gid.x;
    if (row >= params.m || col >= params.n) {
        return;
    }
    var sum: f32 = 0.0;
    for (var i: u32 = 0u; i < params.k; i = i + 1u) {
        sum = sum + a[row * params.k + i] * b[i * params.n + col];
    }
    result[row * params.n + col] = sum;
}
"#;

/// 2D matrix transpose
pub const TRANSPOSE: &str = r#"
@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read_write> result: array<f32>;

struct Params {
    rows: u32,
    cols: u32,
}
@group(0) @binding(2) var<uniform> params: Params;

@compute @workgroup_size(16, 16)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let row = gid.y;
    let col = gid.x;
    if (row >= params.rows || col >= params.cols) {
        return;
    }
    result[col * params.rows + row] = input[row * params.cols + col];
}
"#;
