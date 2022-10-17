//! Multi-dimensional arrays with per-dimension specifiable lower bounds
#![doc(html_root_url = "https://docs.rs/postgres_array/0.10")]

#[doc(inline)]
pub use crate::array::Array;

pub mod array;
mod impls;

/// Information about a dimension of an array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dimension {
    /// The length of the dimension.
    pub len: i32,
    /// The index of the first element of the dimension.
    pub lower_bound: i32,
}

impl Dimension {
    fn shift(&self, idx: i32) -> i32 {
        let offset = self.lower_bound;
        assert!(idx >= offset, "out of bounds array access");
        assert!(
            offset >= 0 || idx <= 0 || i32::max_value() - (-offset) >= idx,
            "out of bounds array access"
        );
        match idx.checked_sub(offset) {
            Some(shifted) => shifted,
            None => panic!("out of bounds array access"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_vec() {
        let a = Array::from_vec(vec![0i32, 1, 2], -1);
        assert!(
            &[Dimension {
                len: 3,
                lower_bound: -1,
            },][..]
                == a.dimensions()
        );
        assert_eq!(0, a[-1]);
        assert_eq!(1, a[0]);
        assert_eq!(2, a[1]);
    }

    #[test]
    fn test_into_inner() {
        let a = Array::from_vec(vec![0i32, 1, 2], -1);
        let a = a.into_inner();
        assert_eq!(a.len(), 3);
        assert_eq!(0, a[0]);
        assert_eq!(1, a[1]);
        assert_eq!(2, a[2]);
    }

    #[test]
    fn test_2d_slice_get() {
        let mut a = Array::from_vec(vec![0i32, 1, 2], -1);
        a.wrap(1);
        assert_eq!(0, a[(1, -1)]);
        assert_eq!(1, a[(1, 0)]);
        assert_eq!(2, a[(1, 1)]);
    }

    #[test]
    #[should_panic]
    fn test_push_wrong_lower_bound() {
        let mut a = Array::from_vec(vec![1i32], -1);
        a.push(Array::from_vec(vec![2], 0));
    }

    #[test]
    #[should_panic]
    fn test_push_wrong_dims() {
        let mut a = Array::from_vec(vec![1i32], -1);
        a.wrap(1);
        a.push(Array::from_vec(vec![1, 2], -1));
    }

    #[test]
    #[should_panic]
    fn test_push_wrong_dim_count() {
        let mut a = Array::from_vec(vec![1i32], -1);
        a.wrap(1);
        let mut b = Array::from_vec(vec![2], -1);
        b.wrap(1);
        a.push(b);
    }

    #[test]
    fn test_push_ok() {
        let mut a = Array::from_vec(vec![1i32, 2], 0);
        a.wrap(0);
        a.push(Array::from_vec(vec![3, 4], 0));
        assert_eq!(1, a[(0, 0)]);
        assert_eq!(2, a[(0, 1)]);
        assert_eq!(3, a[(1, 0)]);
        assert_eq!(4, a[(1, 1)]);
    }

    #[test]
    fn test_3d() {
        let mut a = Array::from_vec(vec![0i32, 1], 0);
        a.wrap(0);
        a.push(Array::from_vec(vec![2, 3], 0));
        a.wrap(0);
        let mut b = Array::from_vec(vec![4, 5], 0);
        b.wrap(0);
        b.push(Array::from_vec(vec![6, 7], 0));
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
        let mut a = Array::from_vec(vec![1i32, 2], 0);
        a.wrap(0);
        a[(0, 0)] = 3;
        assert_eq!(3, a[(0, 0)]);
    }

    #[test]
    fn test_display() {
        let a = Array::from_vec(vec![0i32, 1, 2, 3, 4], 1);
        assert_eq!("{0,1,2,3,4}", &format!("{}", a));

        let a = Array::from_vec(vec![0i32, 1, 2, 3, 4], -3);
        assert_eq!("[-3:1]={0,1,2,3,4}", &format!("{}", a));

        let mut a = Array::from_vec(vec![1i32, 2, 3], 3);
        a.wrap(-2);
        a.push(Array::from_vec(vec![4, 5, 6], 3));
        a.wrap(1);
        assert_eq!("[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}", &format!("{}", a));

        let a: Array<String> = Array::from_parts(vec![], vec![]);
        assert_eq!("{}", &format!("{}", a));
    }
}
