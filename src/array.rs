use std::slice;
use std::vec;

use Dimension;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Array<T> {
    dims: Vec<Dimension>,
    data: Vec<T>,
}

impl<T> Array<T> {
    pub fn from_parts(data: Vec<T>, dimensions: Vec<Dimension>) -> Array<T> {
        assert!((data.is_empty() && dimensions.is_empty()) ||
                data.len() == dimensions.iter().fold(1, |acc, i| acc * i.len),
                "size mismatch");
        Array {
            dims: dimensions,
            data: data,
        }
    }

    pub fn from_vec(data: Vec<T>, lower_bound: isize) -> Array<T> {
        Array {
            dims: vec![Dimension {
                len: data.len(),
                lower_bound: lower_bound
            }],
            data: data,
        }
    }

    pub fn wrap(&mut self, lower_bound: isize) {
        self.dims.insert(0, Dimension {
            len: 1,
            lower_bound: lower_bound,
        });
    }

    pub fn push(&mut self, other: Array<T>) {
        assert!(self.dims.len() - 1 == other.dims.len(),
                "cannot append differently shaped arrays");
        for (dim1, dim2) in self.dims.iter().skip(1).zip(other.dims.iter()) {
            assert!(dim1 == dim2, "cannot append differently shaped arrays");
        }
        self.dims[0].len += 1;
        self.data.extend(other.data);
    }

    pub fn dimensions(&self) -> &[Dimension] {
        &self.dims
    }

    pub fn get(&self, indices: &[isize]) -> &T {
        let idx = self.shift_idx(indices);
        &self.data[idx]
    }

    pub fn get_mut(&mut self, indices: &[isize]) -> &mut T {
        let idx = self.shift_idx(indices);
        &mut self.data[idx]
    }

    fn shift_idx(&self, indices: &[isize]) -> usize {
        assert_eq!(self.dims.len(), indices.len());
        self.dims
            .iter()
            .zip(indices.iter().cloned())
            .fold((0, 1), |(acc, stride), (dim, idx)| {
                let shifted = dim.shift(idx);
                (acc * stride + shifted, dim.len)
            })
            .0
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, T> {
        Iter {
            inner: self.data.iter(),
        }
    }

    pub fn iter_mut<'a>(&'a mut self) -> IterMut<'a, T> {
        IterMut {
            inner: self.data.iter_mut(),
        }
    }
}

impl<'a, T: 'a> IntoIterator for &'a Array<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T: 'a> IntoIterator for &'a mut Array<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> IntoIterator for Array<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            inner: self.data.into_iter()
        }
    }
}

/// An iterator over references to values of an `ArrayBase` in the
/// higher-dimensional equivalent of row-major order.
pub struct Iter<'a, T: 'a> {
    inner: slice::Iter<'a, T>,
}

impl<'a, T: 'a> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        self.inner.next()
    }
}

impl<'a, T: 'a> DoubleEndedIterator for Iter<'a, T> {
    fn next_back(&mut self) -> Option<&'a T> {
        self.inner.next_back()
    }
}

/// An iterator over mutable references to values of an `ArrayBase` in the
/// higher-dimensional equivalent of row-major order.
pub struct IterMut<'a, T: 'a> {
    inner: slice::IterMut<'a, T>,
}

impl<'a, T: 'a> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<&'a mut T> {
        self.inner.next()
    }
}

impl<'a, T: 'a> DoubleEndedIterator for IterMut<'a, T> {
    fn next_back(&mut self) -> Option<&'a mut T> {
        self.inner.next_back()
    }
}

/// An iterator over values of an `ArrayBase` in the higher-dimensional
/// equivalent of row-major order.
pub struct IntoIter<T> {
    inner: vec::IntoIter<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.inner.next()
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        self.inner.next_back()
    }
}
