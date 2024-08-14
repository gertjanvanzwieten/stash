use crate::bytes::Bytes;

pub trait KeyGenerator {
    type Key: Bytes;
    fn digest(&self, b: &[u8]) -> Self::Key;
}

pub struct Blake3;

impl KeyGenerator for Blake3 {
    type Key = [u8; 32];
    fn digest(&self, b: &[u8]) -> Self::Key {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b);
        let mut output = [0; 32];
        let mut output_reader = hasher.finalize_xof();
        output_reader.fill(&mut output);
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt() {
        let h = b"abc";
        assert_eq!(
            Blake3.digest(h).as_bytes(),
            [
                100, 55, 179, 172, 56, 70, 81, 51, 255, 182, 59, 117, 39, 58, 141, 181, 72, 197,
                88, 70, 93, 121, 219, 3, 253, 53, 156, 108, 213, 189, 157, 133
            ]
        )
    }
}
