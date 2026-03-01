# Python FFI Bridge

TL provides a foreign function interface (FFI) to Python, enabling direct interoperation with the Python ecosystem. This is especially useful for leveraging Python libraries such as numpy, pandas, scikit-learn, and others from within TL code.

Requires the `python` feature and Python 3.8+ development headers.

## Building

```
cargo build --features python
```

Ensure Python development headers are installed on your system (e.g., `python3-dev` on Debian/Ubuntu, `python3-devel` on Fedora).

## Core Functions

### py_import

Import a Python module.

```
let np = py_import("numpy")
let pd = py_import("pandas")
let sklearn = py_import("sklearn.linear_model")
```

### py_eval

Evaluate a Python expression and return the result.

```
let result = py_eval("2 ** 10")
let pi = py_eval("__import__('math').pi")
```

### py_call

Call a function on a Python module or object.

```
let np = py_import("numpy")
let arr = py_call(np, "array", [1.0, 2.0, 3.0])
let mean = py_call(np, "mean", arr)
```

### py_getattr

Get an attribute from a Python object.

```
let np = py_import("numpy")
let version = py_getattr(np, "__version__")
```

### py_setattr

Set an attribute on a Python object.

```
py_setattr(obj, "name", "value")
```

### py_to_tl

Convert a Python value to a TL value.

```
let py_list = py_eval("[1, 2, 3]")
let tl_list = py_to_tl(py_list)
```

## Method Calls

Python objects support method call syntax via GetMember/MethodCall dispatch:

```
let np = py_import("numpy")
let arr = py_call(np, "array", [[1.0, 2.0], [3.0, 4.0]])
let transposed = arr.T
let shape = arr.shape
```

## Data Type Conversion

Values are automatically converted between TL and Python types:

| TL Type | Python Type |
|---------|-------------|
| int | int |
| float | float |
| string | str |
| list | list |
| map | dict |
| tensor | numpy.ndarray |

## Tensor and Numpy Interop

TL tensors and numpy arrays are seamlessly convertible. This enables workflows where data is prepared in TL, processed with numpy or scipy, and the results brought back to TL.

```
let np = py_import("numpy")

// Create a numpy array from TL data
let arr = py_call(np, "array", [[1.0, 2.0], [3.0, 4.0]])

// Perform numpy operations
let inv = py_call(np, "linalg.inv", arr)
let det = py_call(np, "linalg.det", arr)

// Convert back to TL tensor
let tl_tensor = py_to_tl(inv)
```

## Example: Using scikit-learn

```
let np = py_import("numpy")
let sklearn = py_import("sklearn.linear_model")

// Prepare training data
let X = py_call(np, "array", [[1.0], [2.0], [3.0], [4.0]])
let y = py_call(np, "array", [2.0, 4.0, 6.0, 8.0])

// Train a linear regression model
let model = py_call(sklearn, "LinearRegression")
py_call(model, "fit", X, y)

// Predict
let X_test = py_call(np, "array", [[5.0], [6.0]])
let predictions = py_call(model, "predict", X_test)
let result = py_to_tl(predictions)
```

## Example: Using pandas

```
let pd = py_import("pandas")

// Read a CSV file with pandas
let df = py_call(pd, "read_csv", "data.csv")

// Access DataFrame methods
let summary = py_call(df, "describe")
let filtered = py_call(df, "query", "age > 30")

// Convert to TL for further processing
let tl_data = py_to_tl(filtered)
```

## Implementation Details

The Python FFI bridge is built on pyo3, a Rust library for Python bindings. The bridge manages the Python GIL (Global Interpreter Lock) and handles reference counting for Python objects.

Python objects in TL are represented as opaque PyObject values. Attribute access and method calls on these objects are dispatched through the GetMember and MethodCall opcodes in the VM.
