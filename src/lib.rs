//! Multi-dimensional arrays with per-dimension specifiable lower bounds
#![doc(html_root_url="https://sfackler.github.io/rust-postgres-array/doc")]

#[macro_use(to_sql_checked)]
extern crate postgres;
extern crate byteorder;

use std::mem;

#[doc(inline)]
pub use base::ArrayBase;

pub mod base;
mod impls;

/// Information about a dimension of an array
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct DimensionInfo {
    /// The size of the dimension
    pub len: usize,
    /// The index of the first element of the dimension
    pub lower_bound: isize,
}

/// Specifies methods that can be performed on multi-dimensional arrays
pub trait Array<T> {
    /// Returns information about the dimensions of this array
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo];

    /// Slices into this array, returning an immutable view of a subarray.
    ///
    /// ## Failure
    ///
    /// Fails if the array is one-dimensional or the index is out of bounds.
    fn slice<'a>(&'a self, idx: isize) -> ArraySlice<'a, T>;

    /// Retrieves an immutable reference to a value in this array.
    ///
    ///
    /// ## Failure
    ///
    /// Fails if the array is multi-dimensional or the index is out of bounds.
    fn get<'a>(&'a self, idx: isize) -> &'a T;
}

/// Specifies methods that can be performed on mutable multi-dimensional arrays
pub trait MutableArray<T> : Array<T> {
    /// Slices into this array, returning a mutable view of a subarray.
    ///
    /// ## Failure
    ///
    /// Fails if the array is one-dimensional or the index is out of bounds.
    fn slice_mut<'a>(&'a mut self, idx: isize) -> MutArraySlice<'a, T>;

    /// Retrieves a mutable reference to a value in this array.
    ///
    ///
    /// ## Failure
    ///
    /// Fails if the array is multi-dimensional or the index is out of bounds.
    fn get_mut<'a>(&'a mut self, idx: isize) -> &'a mut T;
}

#[doc(hidden)]
trait InternalArray<T>: Array<T> {
    fn shift_idx(&self, idx: isize) -> usize {
        let shifted_idx = idx - self.dimension_info()[0].lower_bound;
        assert!(shifted_idx >= 0 &&
                    shifted_idx < self.dimension_info()[0].len as isize,
                "Out of bounds array access");
        shifted_idx as usize
    }

    fn raw_get<'a>(&'a self, idx: usize, size: usize) -> &'a T;
}

#[doc(hidden)]
trait InternalMutableArray<T>: MutableArray<T> {
    fn raw_get_mut<'a>(&'a mut self, idx: usize, size: usize) -> &'a mut T;
}

enum ArrayParent<'parent, T:'parent> {
    Slice(&'parent ArraySlice<'parent, T>),
    MutSlice(&'parent MutArraySlice<'parent, T>),
    Base(&'parent ArrayBase<T>),
}

/// An immutable slice of a multi-dimensional array
pub struct ArraySlice<'parent, T:'parent> {
    parent: ArrayParent<'parent, T>,
    idx: usize,
}

impl<'parent, T> Array<T> for ArraySlice<'parent, T> {
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        let info = match self.parent {
            ArrayParent::Slice(p) => p.dimension_info(),
            ArrayParent::MutSlice(p) => p.dimension_info(),
            ArrayParent::Base(p) => p.dimension_info()
        };
        &info[1..]
    }

    fn slice<'a>(&'a self, idx: isize) -> ArraySlice<'a, T> {
        assert!(self.dimension_info().len() != 1,
                "Attempted to slice a one-dimensional array");
        unsafe {
            ArraySlice {
                parent: ArrayParent::Slice(mem::transmute(self)),
                idx: self.shift_idx(idx),
            }
        }
    }

    fn get<'a>(&'a self, idx: isize) -> &'a T {
        assert!(self.dimension_info().len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<'parent, T> InternalArray<T> for ArraySlice<'parent, T> {
    fn raw_get<'a>(&'a self, idx: usize, size: usize) -> &'a T {
        let size = size * self.dimension_info()[0].len;
        let idx = size * self.idx + idx;
        match self.parent {
            ArrayParent::Slice(p) => p.raw_get(idx, size),
            ArrayParent::MutSlice(p) => p.raw_get(idx, size),
            ArrayParent::Base(p) => p.raw_get(idx, size)
        }
    }
}

enum MutArrayParent<'parent, T:'parent> {
    Slice(&'parent mut MutArraySlice<'parent, T>),
    Base(&'parent mut ArrayBase<T>),
}

/// A mutable slice of a multi-dimensional array
pub struct MutArraySlice<'parent, T:'parent> {
    parent: MutArrayParent<'parent, T>,
    idx: usize,
}

impl<'parent, T> Array<T> for MutArraySlice<'parent, T> {
    fn dimension_info<'a>(&'a self) -> &'a [DimensionInfo] {
        let info : &'a [DimensionInfo] = unsafe {
            match self.parent {
                MutArrayParent::Slice(ref p) => mem::transmute(p.dimension_info()),
                MutArrayParent::Base(ref p) => mem::transmute(p.dimension_info()),
            }
        };
        &info[1..]
    }

    fn slice<'a>(&'a self, idx: isize) -> ArraySlice<'a, T> {
        assert!(self.dimension_info().len() != 1,
                "Attempted to slice a one-dimensional array");
        unsafe {
            ArraySlice {
                parent: ArrayParent::MutSlice(mem::transmute(self)),
                idx: self.shift_idx(idx),
            }
        }
    }

    fn get<'a>(&'a self, idx: isize) -> &'a T {
        assert!(self.dimension_info().len() == 1,
                "Attempted to get from a multi-dimensional array");
        self.raw_get(self.shift_idx(idx), 1)
    }
}

impl<'parent, T> MutableArray<T> for MutArraySlice<'parent, T> {
    fn slice_mut<'a>(&'a mut self, idx: isize) -> MutArraySlice<'a, T> {
        assert!(self.dimension_info().len() != 1,
                "Attempted to slice_mut a one-dimensional array");
        unsafe {
            MutArraySlice {
                idx: self.shift_idx(idx),
                parent: MutArrayParent::Slice(mem::transmute(self)),
            }
        }
    }

    fn get_mut<'a>(&'a mut self, idx: isize) -> &'a mut T {
        assert!(self.dimension_info().len() == 1,
                "Attempted to get_mut from a multi-dimensional array");
        let idx = self.shift_idx(idx);
        self.raw_get_mut(idx, 1)
    }
}

impl<'parent, T> InternalArray<T> for MutArraySlice<'parent, T> {
    fn raw_get<'a>(&'a self, idx: usize, size: usize) -> &'a T {
        let size = size * self.dimension_info()[0].len;
        let idx = size * self.idx + idx;
        unsafe {
            match self.parent {
                MutArrayParent::Slice(ref p) => mem::transmute(p.raw_get(idx, size)),
                MutArrayParent::Base(ref p) => mem::transmute(p.raw_get(idx, size))
            }
        }
    }
}

impl<'parent, T> InternalMutableArray<T> for MutArraySlice<'parent, T> {
    fn raw_get_mut<'a>(&'a mut self, idx: usize, size: usize) -> &'a mut T {
        let size = size * self.dimension_info()[0].len;
        let idx = size * self.idx + idx;
        unsafe {
            match self.parent {
                MutArrayParent::Slice(ref mut p) => mem::transmute(p.raw_get_mut(idx, size)),
                MutArrayParent::Base(ref mut p) => mem::transmute(p.raw_get_mut(idx, size))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_vec() {
        let a = ArrayBase::from_vec(vec!(0i32, 1, 2), -1);
        assert!(&[DimensionInfo { len: 3, lower_bound: -1 }][..] ==
                a.dimension_info());
        assert_eq!(&0, a.get(-1));
        assert_eq!(&1, a.get(0));
        assert_eq!(&2, a.get(1));
    }

    #[test]
    #[should_panic]
    fn test_get_2d_fail() {
        let mut a = ArrayBase::from_vec(vec!(0i32, 1, 2), -1);
        a.wrap(1);
        a.get(1);
    }

    #[test]
    #[should_panic]
    fn test_2d_slice_range_fail_low() {
        let mut a = ArrayBase::from_vec(vec!(0i32, 1, 2), -1);
        a.wrap(1);
        a.slice(0);
    }

    #[test]
    #[should_panic]
    fn test_2d_slice_range_fail_high() {
        let mut a = ArrayBase::from_vec(vec!(0i32, 1, 2), -1);
        a.wrap(1);
        a.slice(2);
    }

    #[test]
    fn test_2d_slice_get() {
        let mut a = ArrayBase::from_vec(vec!(0i32, 1, 2), -1);
        a.wrap(1);
        let s = a.slice(1);
        assert_eq!(&0, s.get(-1));
        assert_eq!(&1, s.get(0));
        assert_eq!(&2, s.get(1));
    }

    #[test]
    #[should_panic]
    fn test_push_move_wrong_lower_bound() {
        let mut a = ArrayBase::from_vec(vec!(1i32), -1);
        a.push_move(ArrayBase::from_vec(vec!(2), 0));
    }

    #[test]
    #[should_panic]
    fn test_push_move_wrong_dims() {
        let mut a = ArrayBase::from_vec(vec!(1i32), -1);
        a.wrap(1);
        a.push_move(ArrayBase::from_vec(vec!(1, 2), -1));
    }

    #[test]
    #[should_panic]
    fn test_push_move_wrong_dim_count() {
        let mut a = ArrayBase::from_vec(vec!(1i32), -1);
        a.wrap(1);
        let mut b = ArrayBase::from_vec(vec!(2), -1);
        b.wrap(1);
        a.push_move(b);
    }

    #[test]
    fn test_push_move_ok() {
        let mut a = ArrayBase::from_vec(vec!(1i32, 2), 0);
        a.wrap(0);
        a.push_move(ArrayBase::from_vec(vec!(3, 4), 0));
        let s = a.slice(0);
        assert_eq!(&1, s.get(0));
        assert_eq!(&2, s.get(1));
        let s = a.slice(1);
        assert_eq!(&3, s.get(0));
        assert_eq!(&4, s.get(1));
    }

    #[test]
    fn test_3d() {
        let mut a = ArrayBase::from_vec(vec!(0i32, 1), 0);
        a.wrap(0);
        a.push_move(ArrayBase::from_vec(vec!(2, 3), 0));
        a.wrap(0);
        let mut b = ArrayBase::from_vec(vec!(4, 5), 0);
        b.wrap(0);
        b.push_move(ArrayBase::from_vec(vec!(6, 7), 0));
        a.push_move(b);
        let s1 = a.slice(0);
        let s2 = s1.slice(0);
        assert_eq!(&0, s2.get(0));
        assert_eq!(&1, s2.get(1));
        let s2 = s1.slice(1);
        assert_eq!(&2, s2.get(0));
        assert_eq!(&3, s2.get(1));
        let s1 = a.slice(1);
        let s2 = s1.slice(0);
        assert_eq!(&4, s2.get(0));
        assert_eq!(&5, s2.get(1));
        let s2 = s1.slice(1);
        assert_eq!(&6, s2.get(0));
        assert_eq!(&7, s2.get(1));
    }

    #[test]
    fn test_mut() {
        let mut a = ArrayBase::from_vec(vec!(1i32, 2), 0);
        a.wrap(0);
        {
            let mut s = a.slice_mut(0);
            *s.get_mut(0) = 3;
        }
        let s = a.slice(0);
        assert_eq!(&3, s.get(0));
    }

    #[test]
    #[should_panic]
    fn test_base_overslice() {
        let a = ArrayBase::from_vec(vec!(1i32), 0);
        a.slice(0);
    }

    #[test]
    #[should_panic]
    fn test_slice_overslice() {
        let mut a = ArrayBase::from_vec(vec!(1i32), 0);
        a.wrap(0);
        let s = a.slice(0);
        s.slice(0);
    }
}
