// ThinkingLanguage — Python FFI Bridge
// Licensed under MIT OR Apache-2.0
//
// Phase 20: Provides Python interoperability via pyo3.
// Feature-gated behind `python` feature flag.

use std::fmt;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PySet, PyString};

use tl_errors::{RuntimeError, TlError};

use crate::value::VmValue;

fn runtime_err(msg: impl Into<String>) -> TlError {
    TlError::Runtime(RuntimeError {
        message: msg.into(),
        span: None,
        stack_trace: vec![],
    })
}

/// Wrapper around a Python object for storage in VmValue.
pub struct PyObjectWrapper {
    pub inner: Py<PyAny>,
}

impl Clone for PyObjectWrapper {
    fn clone(&self) -> Self {
        Python::with_gil(|py| PyObjectWrapper {
            inner: self.inner.clone_ref(py),
        })
    }
}

impl fmt::Debug for PyObjectWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Python::with_gil(|py| {
            let obj = self.inner.bind(py);
            match obj.repr() {
                Ok(r) => write!(f, "{}", r),
                Err(_) => write!(f, "<pyobject>"),
            }
        })
    }
}

impl fmt::Display for PyObjectWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Python::with_gil(|py| {
            let obj = self.inner.bind(py);
            match obj.str() {
                Ok(s) => write!(f, "{}", s),
                Err(_) => write!(f, "<pyobject>"),
            }
        })
    }
}

// --- Value conversion: TL → Python ---

/// Convert a TL VmValue to a Python object.
pub fn vmvalue_to_py(py: Python<'_>, val: &VmValue) -> PyResult<Py<PyAny>> {
    match val {
        VmValue::Int(n) => Ok((*n).into_pyobject(py)?.into_any().into()),
        VmValue::Float(f) => Ok((*f).into_pyobject(py)?.into_any().unbind()),
        VmValue::String(s) => Ok(s.as_ref().into_pyobject(py)?.into_any().unbind()),
        VmValue::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        VmValue::None => Ok(py.None()),
        VmValue::List(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .iter()
                .map(|item| vmvalue_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, &py_items)?.into_any().unbind())
        }
        VmValue::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, v) in pairs {
                let py_val = vmvalue_to_py(py, v)?;
                dict.set_item(k.as_ref(), py_val)?;
            }
            Ok(dict.into_any().unbind())
        }
        VmValue::Set(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .iter()
                .map(|item| vmvalue_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PySet::new(py, &py_items)?.into_any().unbind())
        }
        VmValue::PyObject(wrapper) => Ok(wrapper.inner.clone_ref(py)),
        VmValue::Tensor(tensor) => tensor_to_numpy(py, tensor),
        _ => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "Cannot convert TL {} to Python",
            val.type_name()
        ))),
    }
}

// --- Value conversion: Python → TL ---

/// Convert a Python object to a TL VmValue.
pub fn py_to_vmvalue(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<VmValue> {
    // Order matters: check PyBool before PyInt (bool is subclass of int in Python)
    if obj.is_instance_of::<PyBool>() {
        return Ok(VmValue::Bool(obj.extract::<bool>()?));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(VmValue::Int(obj.extract::<i64>()?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(VmValue::Float(obj.extract::<f64>()?));
    }
    if obj.is_instance_of::<PyString>() {
        let s: String = obj.extract()?;
        return Ok(VmValue::String(Arc::from(s.as_str())));
    }
    if obj.is_none() {
        return Ok(VmValue::None);
    }
    if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>()?;
        let items: Vec<VmValue> = list
            .iter()
            .map(|item| py_to_vmvalue(py, &item))
            .collect::<PyResult<_>>()?;
        return Ok(VmValue::List(items));
    }
    if obj.is_instance_of::<PyDict>() {
        let dict = obj.downcast::<PyDict>()?;
        let mut pairs = Vec::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            let val = py_to_vmvalue(py, &v)?;
            pairs.push((Arc::from(key.as_str()), val));
        }
        return Ok(VmValue::Map(pairs));
    }
    if obj.is_instance_of::<PySet>() {
        let set = obj.downcast::<PySet>()?;
        let mut items = Vec::new();
        for item in set.iter() {
            items.push(py_to_vmvalue(py, &item)?);
        }
        return Ok(VmValue::Set(items));
    }

    // Check for numpy ndarray
    if is_numpy_array(py, obj) {
        return numpy_to_tensor(py, obj);
    }

    // Everything else stays as opaque PyObject
    Ok(VmValue::PyObject(Arc::new(PyObjectWrapper {
        inner: obj.clone().unbind(),
    })))
}

// --- Builtin implementations ---

/// `py_import(module_name)` — import a Python module
pub fn py_import_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.is_empty() {
        return Err(runtime_err("py_import() expects a module name"));
    }
    let name = match &args[0] {
        VmValue::String(s) => s.to_string(),
        _ => return Err(runtime_err("py_import() expects a string module name")),
    };
    Python::with_gil(|py| {
        let module = py
            .import(&*name)
            .map_err(|e| runtime_err(format!("py_import('{name}'): {e}")))?;
        Ok(VmValue::PyObject(Arc::new(PyObjectWrapper {
            inner: module.into_any().unbind(),
        })))
    })
}

/// `py_eval(code)` — evaluate a Python expression and return the result
pub fn py_eval_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.is_empty() {
        return Err(runtime_err("py_eval() expects a code string"));
    }
    let code = match &args[0] {
        VmValue::String(s) => s.to_string(),
        _ => return Err(runtime_err("py_eval() expects a string")),
    };
    Python::with_gil(|py| {
        let result = py
            .eval(&std::ffi::CString::new(code.as_str()).unwrap(), None, None)
            .map_err(|e| runtime_err(format!("py_eval(): {e}")))?;
        py_to_vmvalue(py, &result).map_err(|e| runtime_err(format!("py_eval() conversion: {e}")))
    })
}

/// `py_call(callable, args...)` — call a Python callable with arguments
pub fn py_call_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.is_empty() {
        return Err(runtime_err("py_call() expects a callable and arguments"));
    }
    let callable = match &args[0] {
        VmValue::PyObject(w) => w.clone(),
        _ => {
            return Err(runtime_err(
                "py_call() first argument must be a Python object",
            ));
        }
    };
    let call_args = &args[1..];
    Python::with_gil(|py| {
        let py_args: Vec<Py<PyAny>> = call_args
            .iter()
            .map(|a| vmvalue_to_py(py, a))
            .collect::<Result<_, _>>()
            .map_err(|e| runtime_err(format!("py_call() arg conversion: {e}")))?;
        let tuple = pyo3::types::PyTuple::new(py, &py_args)
            .map_err(|e| runtime_err(format!("py_call() tuple creation: {e}")))?;
        let result = callable
            .inner
            .call1(py, tuple)
            .map_err(|e| runtime_err(format!("py_call(): {e}")))?;
        py_to_vmvalue(py, result.bind(py))
            .map_err(|e| runtime_err(format!("py_call() result conversion: {e}")))
    })
}

/// `py_getattr(obj, name)` — get an attribute from a Python object
pub fn py_getattr_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.len() < 2 {
        return Err(runtime_err("py_getattr() expects (object, name)"));
    }
    let obj = match &args[0] {
        VmValue::PyObject(w) => w.clone(),
        _ => {
            return Err(runtime_err(
                "py_getattr() first argument must be a Python object",
            ));
        }
    };
    let attr_name = match &args[1] {
        VmValue::String(s) => s.to_string(),
        _ => return Err(runtime_err("py_getattr() second argument must be a string")),
    };
    Python::with_gil(|py| {
        let bound = obj.inner.bind(py);
        let attr = bound
            .getattr(attr_name.as_str())
            .map_err(|e| runtime_err(format!("py_getattr('{attr_name}'): {e}")))?;
        py_to_vmvalue(py, &attr).map_err(|e| runtime_err(format!("py_getattr() conversion: {e}")))
    })
}

/// `py_setattr(obj, name, value)` — set an attribute on a Python object
pub fn py_setattr_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.len() < 3 {
        return Err(runtime_err("py_setattr() expects (object, name, value)"));
    }
    let obj = match &args[0] {
        VmValue::PyObject(w) => w.clone(),
        _ => {
            return Err(runtime_err(
                "py_setattr() first argument must be a Python object",
            ));
        }
    };
    let attr_name = match &args[1] {
        VmValue::String(s) => s.to_string(),
        _ => return Err(runtime_err("py_setattr() second argument must be a string")),
    };
    Python::with_gil(|py| {
        let py_val = vmvalue_to_py(py, &args[2])
            .map_err(|e| runtime_err(format!("py_setattr() value conversion: {e}")))?;
        obj.inner
            .bind(py)
            .setattr(attr_name.as_str(), py_val)
            .map_err(|e| runtime_err(format!("py_setattr('{attr_name}'): {e}")))?;
        Ok(VmValue::None)
    })
}

/// `py_to_tl(obj)` — explicitly convert a Python object to a TL value
pub fn py_to_tl_impl(args: &[VmValue]) -> Result<VmValue, TlError> {
    if args.is_empty() {
        return Err(runtime_err("py_to_tl() expects a Python object"));
    }
    match &args[0] {
        VmValue::PyObject(w) => {
            Python::with_gil(|py| {
                let bound = w.inner.bind(py);
                // Try to convert to a native TL value
                // If it's a basic type, convert; otherwise keep as PyObject
                if bound.is_instance_of::<PyBool>() {
                    Ok(VmValue::Bool(bound.extract::<bool>().unwrap_or(false)))
                } else if bound.is_instance_of::<PyInt>() {
                    Ok(VmValue::Int(bound.extract::<i64>().unwrap_or(0)))
                } else if bound.is_instance_of::<PyFloat>() {
                    Ok(VmValue::Float(bound.extract::<f64>().unwrap_or(0.0)))
                } else if bound.is_instance_of::<PyString>() {
                    let s: String = bound.extract().unwrap_or_default();
                    Ok(VmValue::String(Arc::from(s.as_str())))
                } else if bound.is_none() {
                    Ok(VmValue::None)
                } else if bound.is_instance_of::<PyList>() {
                    py_to_vmvalue(py, &bound)
                        .map_err(|e| runtime_err(format!("py_to_tl() conversion: {e}")))
                } else if bound.is_instance_of::<PyDict>() {
                    py_to_vmvalue(py, &bound)
                        .map_err(|e| runtime_err(format!("py_to_tl() conversion: {e}")))
                } else {
                    // Can't convert — keep as PyObject
                    Ok(args[0].clone())
                }
            })
        }
        // Already a TL value — return as-is
        other => Ok(other.clone()),
    }
}

// --- Tensor ↔ numpy interchange ---

/// Check if a Python object is a numpy ndarray
pub fn is_numpy_array(py: Python<'_>, obj: &Bound<'_, PyAny>) -> bool {
    if let Ok(np) = py.import("numpy") {
        if let Ok(ndarray_type) = np.getattr("ndarray") {
            return obj.is_instance(&ndarray_type).unwrap_or(false);
        }
    }
    false
}

/// Convert a TL Tensor to a numpy ndarray
fn tensor_to_numpy(py: Python<'_>, tensor: &tl_ai::TlTensor) -> PyResult<Py<PyAny>> {
    let np = py.import("numpy")?;
    // Get flat data and shape from the TL tensor
    let data: Vec<f64> = tensor.data.iter().copied().collect();
    let shape: Vec<usize> = tensor.shape();

    let py_data = PyList::new(py, &data)?;
    let py_array = np.call_method1("array", (py_data,))?;
    let py_shape = pyo3::types::PyTuple::new(py, &shape)?;
    let reshaped = py_array.call_method1("reshape", (py_shape,))?;
    Ok(reshaped.into_any().unbind())
}

/// Convert a numpy ndarray to a TL Tensor
fn numpy_to_tensor(_py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<VmValue> {
    // Get shape
    let shape_obj = obj.getattr("shape")?;
    let shape: Vec<usize> = shape_obj.extract()?;

    // Flatten and get data as f64 list
    let flat = obj.call_method0("flatten")?;
    let tolist = flat.call_method0("tolist")?;
    let data: Vec<f64> = tolist.extract()?;

    let tensor = tl_ai::TlTensor::from_vec(data, &shape)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;
    Ok(VmValue::Tensor(Arc::new(tensor)))
}

/// Check if a Python object is a pandas DataFrame
pub fn is_pandas_dataframe(py: Python<'_>, obj: &Bound<'_, PyAny>) -> bool {
    if let Ok(pd) = py.import("pandas") {
        if let Ok(df_type) = pd.getattr("DataFrame") {
            return obj.is_instance(&df_type).unwrap_or(false);
        }
    }
    false
}

// --- VM integration helpers ---

/// Get an attribute from a Python object and convert to VmValue.
/// Used by Op::GetMember in vm.rs.
pub fn py_get_member(wrapper: &PyObjectWrapper, field_name: &str) -> VmValue {
    Python::with_gil(|py| {
        let bound = wrapper.inner.bind(py);
        match bound.getattr(field_name) {
            Ok(attr) => py_to_vmvalue(py, &attr).unwrap_or(VmValue::None),
            Err(_) => VmValue::None,
        }
    })
}

/// Call a method on a Python object with TL arguments and return TL result.
/// Used by dispatch_method() in vm.rs.
pub fn py_call_method(
    wrapper: &PyObjectWrapper,
    method: &str,
    args: &[VmValue],
) -> Result<VmValue, TlError> {
    Python::with_gil(|py| {
        let bound = wrapper.inner.bind(py);
        let py_args: Vec<Py<PyAny>> = args
            .iter()
            .map(|a| vmvalue_to_py(py, a))
            .collect::<Result<_, _>>()
            .map_err(|e| runtime_err(format!("Python arg conversion: {e}")))?;
        let tuple = pyo3::types::PyTuple::new(py, &py_args)
            .map_err(|e| runtime_err(format!("Python tuple: {e}")))?;
        let attr = bound
            .getattr(method)
            .map_err(|e| runtime_err(format!("Python: no attribute '{method}': {e}")))?;
        let result = attr
            .call1(tuple)
            .map_err(|e| runtime_err(format!("Python method '{method}': {e}")))?;
        py_to_vmvalue(py, &result)
            .map_err(|e| runtime_err(format!("Python result conversion: {e}")))
    })
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_eval_int() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("1 + 2"))]).unwrap();
        assert!(matches!(result, VmValue::Int(3)));
    }

    #[test]
    fn test_py_eval_string() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("'hello'"))]).unwrap();
        match result {
            VmValue::String(s) => assert_eq!(s.as_ref(), "hello"),
            _ => panic!("Expected String, got {result:?}"),
        }
    }

    #[test]
    fn test_py_eval_float() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("3.14"))]).unwrap();
        match result {
            VmValue::Float(f) => assert!((f - 3.14).abs() < 0.001),
            _ => panic!("Expected Float, got {result:?}"),
        }
    }

    #[test]
    fn test_py_eval_list() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("[1, 2, 3]"))]).unwrap();
        match result {
            VmValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[0], VmValue::Int(1)));
                assert!(matches!(items[1], VmValue::Int(2)));
                assert!(matches!(items[2], VmValue::Int(3)));
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_py_eval_dict() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("{'a': 1}"))]).unwrap();
        match result {
            VmValue::Map(pairs) => {
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].0.as_ref(), "a");
                assert!(matches!(pairs[0].1, VmValue::Int(1)));
            }
            _ => panic!("Expected Map, got {result:?}"),
        }
    }

    #[test]
    fn test_py_eval_none() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("None"))]).unwrap();
        assert!(matches!(result, VmValue::None));
    }

    #[test]
    fn test_py_import_math() {
        pyo3::prepare_freethreaded_python();
        let result = py_import_impl(&[VmValue::String(Arc::from("math"))]).unwrap();
        assert!(matches!(result, VmValue::PyObject(_)));
    }

    #[test]
    fn test_py_getattr() {
        pyo3::prepare_freethreaded_python();
        let math = py_import_impl(&[VmValue::String(Arc::from("math"))]).unwrap();
        let pi = py_getattr_impl(&[math, VmValue::String(Arc::from("pi"))]).unwrap();
        match pi {
            VmValue::Float(f) => assert!((f - std::f64::consts::PI).abs() < 1e-10),
            _ => panic!("Expected Float for pi, got {pi:?}"),
        }
    }

    #[test]
    fn test_py_call_sqrt() {
        pyo3::prepare_freethreaded_python();
        let math = py_import_impl(&[VmValue::String(Arc::from("math"))]).unwrap();
        let sqrt = py_getattr_impl(&[math, VmValue::String(Arc::from("sqrt"))]).unwrap();
        let result = py_call_impl(&[sqrt, VmValue::Float(16.0)]).unwrap();
        match result {
            VmValue::Float(f) => assert!((f - 4.0).abs() < 1e-10),
            _ => panic!("Expected Float 4.0, got {result:?}"),
        }
    }

    #[test]
    fn test_py_error_handling() {
        pyo3::prepare_freethreaded_python();
        let result = py_eval_impl(&[VmValue::String(Arc::from("1/0"))]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("py_eval()") || err_msg.contains("ZeroDivision"));
    }
}
