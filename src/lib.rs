//! Multi-dimensional arrays with per-dimension specifiable lower bounds
#![doc(html_root_url="https://sfackler.github.io/rust-postgres-array/doc/v0.5.0")]

#[macro_use(to_sql_checked)]
extern crate postgres;
extern crate byteorder;

#[doc(inline)]
pub use array::Array;

pub mod array;
mod impls;

/// Information about a dimension of an array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dimension {
    /// The length of the dimension.
    pub len: usize,
    /// The index of the first element of the dimension.
    pub lower_bound: isize,
}

impl Dimension {
    fn shift(&self, idx: isize) -> usize {
        let offset = self.lower_bound;
        assert!(idx >= offset, "out of bounds array access");
        assert!(offset >= 0 || idx <= 0 || usize::max_value() - (-offset) as usize >= idx as usize,
                "out of bounds array access");
        let shifted = idx.wrapping_sub(offset) as usize;
        assert!(shifted < self.len, "out of bounds array access");
        shifted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_vec() {
        let a = Array::from_vec(vec!(0i32, 1, 2), -1);
        assert!(&[Dimension { len: 3, lower_bound: -1 }][..] ==
                a.dimensions());
        assert_eq!(0, a[-1]);
        assert_eq!(1, a[0]);
        assert_eq!(2, a[1]);
    }

    #[test]
    fn test_2d_slice_get() {
        let mut a = Array::from_vec(vec!(0i32, 1, 2), -1);
        a.wrap(1);
        assert_eq!(0, a[(1, -1)]);
        assert_eq!(1, a[(1, 0)]);
        assert_eq!(2, a[(1, 1)]);
    }

    #[test]
    #[should_panic]
    fn test_push_wrong_lower_bound() {
        let mut a = Array::from_vec(vec!(1i32), -1);
        a.push(Array::from_vec(vec!(2), 0));
    }

    #[test]
    #[should_panic]
    fn test_push_wrong_dims() {
        let mut a = Array::from_vec(vec!(1i32), -1);
        a.wrap(1);
        a.push(Array::from_vec(vec!(1, 2), -1));
    }

    #[test]
    #[should_panic]
    fn test_push_wrong_dim_count() {
        let mut a = Array::from_vec(vec!(1i32), -1);
        a.wrap(1);
        let mut b = Array::from_vec(vec!(2), -1);
        b.wrap(1);
        a.push(b);
    }

    #[test]
    fn test_push_ok() {
        let mut a = Array::from_vec(vec!(1i32, 2), 0);
        a.wrap(0);
        a.push(Array::from_vec(vec!(3, 4), 0));
        assert_eq!(1, a[(0, 0)]);
        assert_eq!(2, a[(0, 1)]);
        assert_eq!(3, a[(1, 0)]);
        assert_eq!(4, a[(1, 1)]);
    }

    #[test]
    fn test_3d() {
        let mut a = Array::from_vec(vec!(0i32, 1), 0);
        a.wrap(0);
        a.push(Array::from_vec(vec!(2, 3), 0));
        a.wrap(0);
        let mut b = Array::from_vec(vec!(4, 5), 0);
        b.wrap(0);
        b.push(Array::from_vec(vec!(6, 7), 0));
        a.push(b);
        assert_eq!(0, a[(0, 0, 0)]);
        assert_eq!(1, a[(0, 0, 1)]);
        assert_eq!(2, a[(0, 1, 0)]);
        assert_eq!(3, a[(0, 1, 1)]);
        assert_eq!(4, a[(1, 0, 0)]);
        assert_eq!(5, a[(1, 0, 1)]);
        assert_eq!(6, a[(1, 1, 0)]);
        assert_eq!(7, a[(1, 1, 1)]);
    }

    #[test]
    fn test_mut() {
        let mut a = Array::from_vec(vec!(1i32, 2), 0);
        a.wrap(0);
        a[(0, 0)] = 3;
        assert_eq!(3, a[(0, 0)]);
    }
}
