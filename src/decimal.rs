use failure::{Error, Fail};
use num_bigint::BigInt;

#[derive(Debug, Clone)]
pub struct Decimal {
    value: BigInt,
    len: usize,
}

// We only care about value equality, not byte length. Can two equal `BigInt`s have two different
// byte lengths?
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

#[derive(Fail, Debug)]
#[fail(display = "Decimal sign extension error: {}", _0)]
pub struct SignExtendError(&'static str, usize, usize);

impl Decimal {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn to_vec(&self) -> Result<Vec<u8>, Error> {
        self.to_sign_extended_bytes_with_len(self.len)
    }

    pub(crate) fn to_sign_extended_bytes_with_len(&self, len: usize) -> Result<Vec<u8>, Error> {
        let sign_byte = 0xFF * u8::from(self.value.sign() == num_bigint::Sign::Minus);
        let mut decimal_bytes = vec![sign_byte; len];
        let raw_bytes = self.value.to_signed_bytes_be();
        let num_raw_bytes = raw_bytes.len();
        let start_byte_index = len.checked_sub(num_raw_bytes).ok_or_else(|| {
            SignExtendError(
                "number of bytes requested for sign extension {} is less than the number of bytes needed to decode {}",
                len,
                num_raw_bytes,
            )
        })?;
        decimal_bytes[start_byte_index..].copy_from_slice(&raw_bytes);
        Ok(decimal_bytes)
    }
}

impl std::convert::TryFrom<&Decimal> for Vec<u8> {
    type Error = Error;

    fn try_from(decimal: &Decimal) -> Result<Self, Self::Error> {
        decimal.to_vec()
    }
}

impl From<Vec<u8>> for Decimal {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            value: num_bigint::BigInt::from_signed_bytes_be(&bytes),
            len: bytes.len(),
        }
    }
}
