//! Ancillary representation

use std::fmt;

use libc::{IPPROTO_IP, IPPROTO_IPV6, IPV6_HOPLIMIT, IPV6_TCLASS, IP_TOS, IP_TTL};

/// Enum representing the different values of the ECN field
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ECNValue {
    /// Not ECN-Capable Transport
    None,
    /// ECN Capable Transport(0) (ECT(0))
    ECT0,
    /// ECN Capable Transport(1) (ECT(1))
    ECT1,
    /// Congestion Experienced (CE)
    CE
}

impl From<*mut u8> for ECNValue{
    fn from(value: *mut u8) -> Self {
        let value = unsafe { *value };
        match value & 0b11 {
            0b00 => Self::None,
            0b01 => Self::ECT1,
            0b10 => Self::ECT0,
            0b11 => Self::CE,
            _ => unreachable!()
        }
    }
}

impl From<ECNValue> for u32{
    fn from(val: ECNValue) -> Self {
        match val {
            ECNValue::None => 0b00,
            ECNValue::ECT0 => 0b10,
            ECNValue::ECT1 => 0b01,
            ECNValue::CE => 0b11,
        }
    }
}

/// Failed to parse an Ancillary
pub struct AncillaryError(i32, i32);

impl fmt::Display for AncillaryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid ancillary received: level = {}, type = {}", self.0, self.1)
    }
}

/// Structure representing the different ancillaries which can be passed to quiche
#[derive(Debug, Clone)]
pub enum Ancillary{
    /// TTL ancillary
    TTL(u32),
    /// ECN ancillary
    ECN(ECNValue)
}

impl Ancillary {

    /// Parse an ancillary from raw data (level, type, data) that was received
    /// by a recvmsg syscall
    pub fn from_raw(cmsg_level: i32, cmsg_type: i32, data: *mut u8) -> Result<Self, AncillaryError>{
        match (cmsg_level, cmsg_type){
            (IPPROTO_IP, IP_TOS) => Ok(Self::ECN(data.into())),
            (IPPROTO_IPV6, IPV6_TCLASS) => Ok(Self::ECN(data.into())),
            (IPPROTO_IP, IP_TTL) => Ok(Self::TTL(unsafe { *data } as u32)),
            (IPPROTO_IPV6, IPV6_HOPLIMIT) => Ok(Self::TTL(unsafe { *data } as u32)),
            _ => Err(AncillaryError(cmsg_level, cmsg_type))
        }
    }
}
