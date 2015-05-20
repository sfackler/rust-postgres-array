use std::slice;
use std::vec;

use {DimensionInfo,
     Array,
     MutableArray,
     InternalArray,
     InternalMutableArray,
     ArraySlice,
     MutArraySlice,
     ArrayParent,
     MutArrayParent};

/// A multi-dimensional array.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ArrayBase<T> {
    info: Vec<DimensionInfo>,
    data: Vec<T>,
}

impl<T> ArrayBase<T> {
    /// Creates a new multi-dimensional array from its underlying components.
    ///
    /// The data array should be provided in the higher-dimensional equivalent
    /// of row-major order.
    ///
    /// ## Failure
    ///
    /// Fails if there are 0 dimensions or the number of elements provided does
    /// not match the number of elements specified.
    pub fn from_raw(data: Vec<T>, info: Vec<DimensionInfo>)
            -> ArrayBase<T> {
        assert!((data.is_empty() && info.is_empty()) ||
                data.len() == info.iter().fold(1, |acc, i| acc * i.len),
                "Size mismatch");
        ArrayBase {
            info: info,
            data: data,
        }
    }

    /// Creates a new one-dimensional array from a vector.
    pub fn from_vec(data: Vec<T>, lower_bound: isize) -> ArrayBase<T> {
        ArrayBase {
            info: vec!(DimensionInfo {
                len: data.len(),
                lower_bound: lower_bound
            }),
            data: data
        }
    }

    /// Wraps this array in a new dimension of size 1.
    ///
    /// For example the one-dimensional array `[1,2]` would turn into
    /// the two-dimensional array `[[1,2]]`.
    pub fn wrap(&mut self, lower_bound: isize) {
        self.info.insert(0, DimensionInfo {
            len: 1,
            lower_bound: lower_bound
        })
    }

    /// Takes ownership of another array, appending it to the top-level
    /// dimension of this array.
    ///
    /// The dimensions of the other array must have an identical shape to the
    /// dimensions of a slice of this array. This includes both the sizes of
    /// the dimensions as well as their lower bounds.
    ///
    /// For example, if `[3,4]` is pushed onto `[[1,2]]`, the result is
    /// `[[1,2],[3,4]]`.
    ///
    /// ## Failure
    ///
    /// Fails if the other array does not have dimensions identical to the
    /// dimensions of a slice of this array.
    pub fn push_move(&mut self, other: ArrayBase<T>) {
        assert!(self.info.len() - 1 == other.info.len(),
                "Cannot append differently shaped arrays");
        for (info1, info2) in self.info.iter().skip(1).zip(other.info.iter()) {
            assert!(info1 == info2, "Cannot join differently shaped arrays");
        }
        self.info[0].len += 1;
        self.data.extend(other.data.into_iter());
    }

    /// Returns an iterator over references to the values in this array, in the
    /// higher-dimensional equivalent of row-major order.
    pub fn iter<'a>(&'a self) -> Iter<'a, T> {
        Iter {
            inner: self.data.iter(),
        }
    }

    /// Returns an iterator over references to the values in this array, in the
    /// higher-dimensional equivalent of row-major order.
    pub fn iter_mut<'a>(&'a mut self) -> IterMut<'a, T> {
        IterMut {
            inner: self.data.iter_mut(),
        }
    }
}

impl<'a, T: 'a> IntoIterator for &'a ArrayBase<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T: 'a> IntoIterator for &'a mut ArrayBase<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> IntoIterator for ArrayBase<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            inner: self.data.into_iter()
        }
    }
}

impl<T> Array<T> for ArrayBase<T> {
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        &*self.info
    }

    fn slice<'a>(&'a self, idx: isize) -> ArraySlice<'a, T> {
        assert!(self.info.len() != 1,
                "Attempted to slice a one-dimensional array");
        ArraySlice {
            parent: ArrayParent::Base(self),
            idx: self.shift_idx(idx),
        }
    }

    fn get<'a>(&'a self, idx: isize) -> &'a T {
        assert!(self.info.len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<T> MutableArray<T> for ArrayBase<T> {
    fn slice_mut<'a>(&'a mut self, idx: isize) -> MutArraySlice<'a, T> {
        assert!(self.info.len() != 1,
                "Attempted to slice_mut into a one-dimensional array");
        MutArraySlice {
            idx: self.shift_idx(idx),
            parent: MutArrayParent::Base(self),
        }
    }

    fn get_mut<'a>(&'a mut self, idx: isize) -> &'a mut T {
        assert!(self.info.len() == 1,
                "Attempted to get_mut from a multi-dimensional array");
        let idx = self.shift_idx(idx);
        self.raw_get_mut(idx, 1)
    }
}

impl<T> InternalArray<T> for ArrayBase<T> {
    fn raw_get<'a>(&'a self, idx: usize, _size: usize) -> &'a T {
        &self.data[idx]
    }
}

impl<T> InternalMutableArray<T> for ArrayBase<T> {
    fn raw_get_mut<'a>(&'a mut self, idx: usize, _size: usize) -> &'a mut T {
        &mut self.data[idx]
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
