use std::hash::{BuildHasher, Hasher};

#[derive(Default)]
pub struct NoHashBuilder;

impl BuildHasher for NoHashBuilder {
    type Hasher = NoHasher;
    fn build_hasher(&self) -> Self::Hasher {
        NoHasher([0; 8])
    }
}

pub struct NoHasher([u8; 8]);

impl Hasher for NoHasher {
    fn finish(&self) -> u64 {
        u64::from_le_bytes(self.0)
    }
    fn write(&mut self, bytes: &[u8]) {
        let n = bytes.len();
        if n >= 8 {
            self.0[..].copy_from_slice(&bytes[n - 8..]);
        } else {
            self.0.copy_within(n..8, 0);
            self.0[8 - n..].copy_from_slice(bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write() {
        let mut n = NoHasher([0; 8]);
        n.write(&[1, 2, 3]);
        assert_eq!(n.finish(), 0x_03_02_01_00_00_00_00_00)
    }
    #[test]
    fn test_copy_within() {
        let mut n = NoHasher([0; 8]);
        n.write(&[1, 2, 3]);
        n.write(&[4, 5, 6]);
        assert_eq!(n.finish(), 0x_06_05_04_03_02_01_00_00)
    }
    #[test]
    fn test_overflow() {
        let mut n = NoHasher([0; 8]);
        n.write(&[1, 2, 3, 4, 5, 6]);
        n.write(&[7, 8, 9]);
        assert_eq!(n.finish(), 0x_09_08_07_06_05_04_03_02)
    }
    #[test]
    fn test_long_write() {
        let mut n = NoHasher([0; 8]);
        n.write(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(n.finish(), 0x_09_08_07_06_05_04_03_02)
    }
}
