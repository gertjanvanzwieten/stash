pub trait Bytes: Sized + Eq + Clone {
    const NBYTES: usize;
    fn as_bytes(&self) -> &[u8];
    fn from_bytes(b: &[u8]) -> Option<Self>;
}

impl<const N: usize> Bytes for [u8; N] {
    const NBYTES: usize = N;
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
    fn from_bytes(b: &[u8]) -> Option<Self> {
        b.try_into().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nbytes() {
        assert_eq!(<[u8; 3]>::NBYTES, 3)
    }
    #[test]
    fn test_as_bytes() {
        let h = [1, 2, 3];
        assert_eq!(h.as_bytes(), &[1, 2, 3])
    }
    #[test]
    fn test_from_bytes() {
        let b = <[u8; 3]>::from_bytes(&[1, 2, 3]);
        assert_eq!(b, Some([1, 2, 3]));
    }
    #[test]
    fn test_from_bytes_too_short() {
        let b = <[u8; 4]>::from_bytes(&[1, 2, 3]);
        assert_eq!(b, None)
    }
    #[test]
    fn test_from_bytes_too_long() {
        let b = <[u8; 2]>::from_bytes(&[1, 2, 3]);
        assert_eq!(b, None)
    }
}
