pub struct Encode(usize, bool);

pub fn encode(value: usize) -> Encode {
    Encode(value, true)
}

impl Iterator for Encode {
    type Item = u8;
    fn next(&mut self) -> Option<Self::Item> {
        if self.1 {
            let mut b = self.0 as u8;
            self.0 >>= 7;
            if self.0 == 0 {
                self.1 = false;
            } else {
                b |= 0x80;
            }
            Some(b)
        } else {
            None
        }
    }
}

pub fn decode(it: &mut impl std::iter::Iterator<Item = u8>) -> usize {
    let mut b = it.next().expect("iterator exhausted before decoding usize");
    let mut u = (b & 0x7f) as usize;
    let mut n = 0;
    while b & 0x80 != 0 {
        n += 7;
        b = it.next().expect("iterator exhausted while decoding usize");
        u |= ((b & 0x7f) as usize) << n;
    }
    u
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test(i: usize, n: usize) {
        let v: Vec<u8> = encode(i).collect();
        println!("{:?}", v);
        assert_eq!(v.len(), n);
        let mut it = v.into_iter();
        assert_eq!(decode(&mut it), i);
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_0() {
        test(0, 1)
    }

    #[test]
    fn test_1() {
        test(1, 1)
    }

    #[test]
    fn test_7f() {
        test(127, 1)
    }

    #[test]
    fn test_80() {
        test(128, 2)
    }

    #[test]
    fn test_3fff() {
        test(16383, 2)
    }

    #[test]
    fn test_4000() {
        test(16384, 3)
    }
}
