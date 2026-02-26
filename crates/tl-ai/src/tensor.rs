// ThinkingLanguage — Tensor type
// Wraps ndarray::ArrayD<f64> for numerical computing.

use ndarray::{ArrayD, IxDyn, Axis};
use std::fmt;

/// A dynamically-shaped tensor of f64 values.
#[derive(Clone)]
pub struct TlTensor {
    pub data: ArrayD<f64>,
    pub name: Option<String>,
}

impl fmt::Debug for TlTensor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Tensor(shape={:?})", self.data.shape())
    }
}

impl fmt::Display for TlTensor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shape = self.data.shape();
        if shape.len() == 1 && shape[0] <= 10 {
            write!(f, "tensor([")?;
            for (i, v) in self.data.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                if v.fract() == 0.0 {
                    write!(f, "{v:.1}")?;
                } else {
                    write!(f, "{v}")?;
                }
            }
            write!(f, "])")
        } else {
            write!(f, "tensor(shape={:?})", shape)
        }
    }
}

impl TlTensor {
    /// Create a tensor filled with zeros.
    pub fn zeros(shape: &[usize]) -> Self {
        TlTensor {
            data: ArrayD::zeros(IxDyn(shape)),
            name: None,
        }
    }

    /// Create a tensor filled with ones.
    pub fn ones(shape: &[usize]) -> Self {
        TlTensor {
            data: ArrayD::ones(IxDyn(shape)),
            name: None,
        }
    }

    /// Create a tensor from a flat Vec and a shape.
    pub fn from_vec(data: Vec<f64>, shape: &[usize]) -> Result<Self, String> {
        let expected: usize = shape.iter().product();
        if data.len() != expected {
            return Err(format!(
                "Shape {:?} requires {} elements, got {}",
                shape,
                expected,
                data.len()
            ));
        }
        let arr = ArrayD::from_shape_vec(IxDyn(shape), data)
            .map_err(|e| format!("Failed to create tensor: {e}"))?;
        Ok(TlTensor {
            data: arr,
            name: None,
        })
    }

    /// Create a 1D tensor from a list of f64 values.
    pub fn from_list(data: Vec<f64>) -> Self {
        let len = data.len();
        TlTensor {
            data: ArrayD::from_shape_vec(IxDyn(&[len]), data).unwrap(),
            name: None,
        }
    }

    /// Get the shape as a Vec.
    pub fn shape(&self) -> Vec<usize> {
        self.data.shape().to_vec()
    }

    /// Reshape the tensor.
    pub fn reshape(&self, new_shape: &[usize]) -> Result<Self, String> {
        let new_data = self
            .data
            .clone()
            .into_shape(IxDyn(new_shape))
            .map_err(|e| format!("Reshape failed: {e}"))?;
        Ok(TlTensor {
            data: new_data,
            name: self.name.clone(),
        })
    }

    /// Transpose a 2D tensor.
    pub fn transpose(&self) -> Result<Self, String> {
        if self.data.ndim() != 2 {
            return Err(format!(
                "Transpose requires 2D tensor, got {}D",
                self.data.ndim()
            ));
        }
        let transposed = self.data.clone().reversed_axes();
        Ok(TlTensor {
            data: transposed,
            name: self.name.clone(),
        })
    }

    /// Flatten to 1D.
    pub fn flatten(&self) -> Self {
        let flat: Vec<f64> = self.data.iter().cloned().collect();
        TlTensor::from_list(flat)
    }

    /// Sum of all elements.
    pub fn sum(&self) -> f64 {
        self.data.sum()
    }

    /// Mean of all elements.
    pub fn mean(&self) -> f64 {
        let n = self.data.len() as f64;
        if n == 0.0 {
            0.0
        } else {
            self.data.sum() / n
        }
    }

    /// Minimum element.
    pub fn min(&self) -> f64 {
        self.data
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min)
    }

    /// Maximum element.
    pub fn max(&self) -> f64 {
        self.data
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max)
    }

    /// Get element by flat index for 1D tensors or multi-index.
    pub fn get(&self, indices: &[usize]) -> Option<f64> {
        self.data.get(IxDyn(indices)).cloned()
    }

    /// Slice along first axis.
    pub fn slice(&self, start: usize, end: usize) -> Result<Self, String> {
        if self.data.ndim() == 0 {
            return Err("Cannot slice a scalar tensor".to_string());
        }
        let sliced = self.data.slice_axis(Axis(0), ndarray::Slice::from(start..end));
        Ok(TlTensor {
            data: sliced.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Convert to a flat Vec<f64>.
    pub fn to_vec(&self) -> Vec<f64> {
        self.data.iter().cloned().collect()
    }

    /// Element-wise addition.
    pub fn add(&self, other: &TlTensor) -> Result<Self, String> {
        let result = &self.data + &other.data;
        Ok(TlTensor {
            data: result,
            name: None,
        })
    }

    /// Element-wise subtraction.
    pub fn sub(&self, other: &TlTensor) -> Result<Self, String> {
        let result = &self.data - &other.data;
        Ok(TlTensor {
            data: result,
            name: None,
        })
    }

    /// Element-wise multiplication.
    pub fn mul(&self, other: &TlTensor) -> Result<Self, String> {
        let result = &self.data * &other.data;
        Ok(TlTensor {
            data: result,
            name: None,
        })
    }

    /// Element-wise division.
    pub fn div(&self, other: &TlTensor) -> Result<Self, String> {
        let result = &self.data / &other.data;
        Ok(TlTensor {
            data: result,
            name: None,
        })
    }

    /// Matrix multiplication (dot product) for 1D or 2D tensors.
    pub fn dot(&self, other: &TlTensor) -> Result<Self, String> {
        // 1D dot 1D → scalar
        if self.data.ndim() == 1 && other.data.ndim() == 1 {
            let a = self.data.as_slice().ok_or("Non-contiguous tensor")?;
            let b = other.data.as_slice().ok_or("Non-contiguous tensor")?;
            if a.len() != b.len() {
                return Err(format!(
                    "Dot product dimension mismatch: {} vs {}",
                    a.len(),
                    b.len()
                ));
            }
            let result: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            Ok(TlTensor {
                data: ArrayD::from_elem(IxDyn(&[]), result),
                name: None,
            })
        }
        // 2D dot 2D → matrix multiply
        else if self.data.ndim() == 2 && other.data.ndim() == 2 {
            let a = self
                .data
                .view()
                .into_dimensionality::<ndarray::Ix2>()
                .map_err(|e| format!("Shape error: {e}"))?;
            let b = other
                .data
                .view()
                .into_dimensionality::<ndarray::Ix2>()
                .map_err(|e| format!("Shape error: {e}"))?;
            let c = a.dot(&b);
            Ok(TlTensor {
                data: c.into_dyn(),
                name: None,
            })
        } else {
            Err(format!(
                "Dot product not supported for {}D and {}D tensors",
                self.data.ndim(),
                other.data.ndim()
            ))
        }
    }

    /// Scalar multiplication.
    pub fn scale(&self, scalar: f64) -> Self {
        TlTensor {
            data: &self.data * scalar,
            name: self.name.clone(),
        }
    }

    /// Cosine similarity between two 1D tensors.
    pub fn cosine_similarity(&self, other: &TlTensor) -> Result<f64, String> {
        let a = self.data.as_slice().ok_or("Non-contiguous tensor")?;
        let b = other.data.as_slice().ok_or("Non-contiguous tensor")?;
        if a.len() != b.len() {
            return Err(format!(
                "Dimension mismatch for cosine similarity: {} vs {}",
                a.len(),
                b.len()
            ));
        }
        let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
        let norm_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            return Ok(0.0);
        }
        Ok(dot / (norm_a * norm_b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zeros_ones() {
        let z = TlTensor::zeros(&[2, 3]);
        assert_eq!(z.shape(), vec![2, 3]);
        assert_eq!(z.sum(), 0.0);

        let o = TlTensor::ones(&[2, 3]);
        assert_eq!(o.sum(), 6.0);
    }

    #[test]
    fn test_from_vec() {
        let t = TlTensor::from_vec(vec![1.0, 2.0, 3.0, 4.0], &[2, 2]).unwrap();
        assert_eq!(t.shape(), vec![2, 2]);
        assert_eq!(t.get(&[0, 0]), Some(1.0));
        assert_eq!(t.get(&[1, 1]), Some(4.0));
    }

    #[test]
    fn test_from_list() {
        let t = TlTensor::from_list(vec![1.0, 2.0, 3.0]);
        assert_eq!(t.shape(), vec![3]);
        assert_eq!(t.sum(), 6.0);
    }

    #[test]
    fn test_arithmetic() {
        let a = TlTensor::from_list(vec![1.0, 2.0, 3.0]);
        let b = TlTensor::from_list(vec![4.0, 5.0, 6.0]);

        let sum = a.add(&b).unwrap();
        assert_eq!(sum.to_vec(), vec![5.0, 7.0, 9.0]);

        let diff = a.sub(&b).unwrap();
        assert_eq!(diff.to_vec(), vec![-3.0, -3.0, -3.0]);

        let prod = a.mul(&b).unwrap();
        assert_eq!(prod.to_vec(), vec![4.0, 10.0, 18.0]);

        let quot = b.div(&a).unwrap();
        assert_eq!(quot.to_vec(), vec![4.0, 2.5, 2.0]);
    }

    #[test]
    fn test_reshape() {
        let t = TlTensor::from_vec(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], &[2, 3]).unwrap();
        let r = t.reshape(&[3, 2]).unwrap();
        assert_eq!(r.shape(), vec![3, 2]);
        assert_eq!(r.get(&[0, 0]), Some(1.0));
    }

    #[test]
    fn test_transpose() {
        let t = TlTensor::from_vec(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], &[2, 3]).unwrap();
        let tr = t.transpose().unwrap();
        assert_eq!(tr.shape(), vec![3, 2]);
        assert_eq!(tr.get(&[0, 0]), Some(1.0));
        assert_eq!(tr.get(&[0, 1]), Some(4.0));
    }

    #[test]
    fn test_dot_1d() {
        let a = TlTensor::from_list(vec![1.0, 2.0, 3.0]);
        let b = TlTensor::from_list(vec![4.0, 5.0, 6.0]);
        let dot = a.dot(&b).unwrap();
        assert_eq!(dot.sum(), 32.0); // 1*4 + 2*5 + 3*6
    }

    #[test]
    fn test_dot_2d() {
        let a = TlTensor::from_vec(vec![1.0, 2.0, 3.0, 4.0], &[2, 2]).unwrap();
        let b = TlTensor::from_vec(vec![5.0, 6.0, 7.0, 8.0], &[2, 2]).unwrap();
        let c = a.dot(&b).unwrap();
        assert_eq!(c.shape(), vec![2, 2]);
        assert_eq!(c.get(&[0, 0]), Some(19.0)); // 1*5 + 2*7
        assert_eq!(c.get(&[0, 1]), Some(22.0)); // 1*6 + 2*8
    }

    #[test]
    fn test_reductions() {
        let t = TlTensor::from_list(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(t.sum(), 15.0);
        assert_eq!(t.mean(), 3.0);
        assert_eq!(t.min(), 1.0);
        assert_eq!(t.max(), 5.0);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = TlTensor::from_list(vec![1.0, 0.0]);
        let b = TlTensor::from_list(vec![1.0, 0.0]);
        let sim = a.cosine_similarity(&b).unwrap();
        assert!((sim - 1.0).abs() < 1e-10);

        let c = TlTensor::from_list(vec![0.0, 1.0]);
        let sim2 = a.cosine_similarity(&c).unwrap();
        assert!(sim2.abs() < 1e-10); // orthogonal
    }

    #[test]
    fn test_scale() {
        let t = TlTensor::from_list(vec![1.0, 2.0, 3.0]);
        let scaled = t.scale(2.0);
        assert_eq!(scaled.to_vec(), vec![2.0, 4.0, 6.0]);
    }

    #[test]
    fn test_flatten() {
        let t = TlTensor::from_vec(vec![1.0, 2.0, 3.0, 4.0], &[2, 2]).unwrap();
        let flat = t.flatten();
        assert_eq!(flat.shape(), vec![4]);
        assert_eq!(flat.to_vec(), vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_slice() {
        let t = TlTensor::from_list(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let sliced = t.slice(1, 4).unwrap();
        assert_eq!(sliced.to_vec(), vec![20.0, 30.0, 40.0]);
    }

    #[test]
    fn test_display() {
        let t = TlTensor::from_list(vec![1.0, 2.0, 3.0]);
        let s = format!("{t}");
        assert_eq!(s, "tensor([1.0, 2.0, 3.0])");
    }
}
