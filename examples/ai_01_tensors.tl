// ThinkingLanguage — AI Example 01: Tensors
// Demonstrates tensor creation, arithmetic, and operations.

// Create tensors from lists
let a = tensor([1.0, 2.0, 3.0, 4.0])
let b = tensor([5.0, 6.0, 7.0, 8.0])

// Basic info
print("Tensor a:", a)
print("Shape:", tensor_shape(a))
print("Sum:", tensor_sum(a))
print("Mean:", tensor_mean(a))

// Element-wise arithmetic
let added = a + b
let subtracted = a - b
let multiplied = a * b
print("a + b:", added)
print("a - b:", subtracted)
print("a * b:", multiplied)

// Scalar multiplication
let scaled = a * 3.0
print("a * 3:", scaled)

// Dot product
let dot = tensor_dot(a, b)
print("a dot b:", dot)

// Create zeros and ones
let zeros = tensor_zeros([2, 3])
let ones = tensor_ones([2, 3])
print("Zeros shape:", tensor_shape(zeros))
print("Ones sum:", tensor_sum(ones))

// Reshape
let mat = tensor([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], [2, 3])
print("Matrix:", mat)
print("Matrix shape:", tensor_shape(mat))

let reshaped = tensor_reshape(mat, [3, 2])
print("Reshaped:", tensor_shape(reshaped))

// Transpose
let transposed = tensor_transpose(mat)
print("Transposed shape:", tensor_shape(transposed))

// Cosine similarity
let v1 = tensor([1.0, 0.0, 0.0])
let v2 = tensor([0.0, 1.0, 0.0])
let v3 = tensor([1.0, 0.0, 0.0])
print("similarity(v1, v2) — orthogonal:", similarity(v1, v2))
print("similarity(v1, v3) — identical:", similarity(v1, v3))
