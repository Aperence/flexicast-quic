use std::{io, net::Ipv4Addr, os::fd::AsRawFd};
use libc::{c_int, in_addr, ip_mreq_source, socklen_t, IPPROTO_IP, IP_ADD_SOURCE_MEMBERSHIP, IP_DROP_SOURCE_MEMBERSHIP};

pub trait SSM{
    fn join_ssm_multicast_v4(&self, group: &Ipv4Addr, itf: &Ipv4Addr, source: &Ipv4Addr) -> io::Result<()>;
    fn leave_ssm_multicast_v4(&self, group: &Ipv4Addr, itf: &Ipv4Addr, source: &Ipv4Addr) -> io::Result<()>;
}

// https://github.com/rust-lang/rust/blob/master/library/std/src/sys/net/connection/socket.rs ip_v4_addr_to_c
fn ip_v4_addr_to_c(addr: &Ipv4Addr) -> in_addr {
    // `s_addr` is stored as BE on all machines and the array is in BE order.
    // So the native endian conversion method is used so that it's never swapped.
    in_addr { s_addr: u32::from_ne_bytes(addr.octets()) }
}

// https://github.com/rust-lang/rust/blob/master/library/std/src/sys/net/connection/socket.rs setsockopt
pub fn setsockopt<T>(
    sock: &impl AsRawFd,
    level: c_int,
    option_name: c_int,
    option_value: T,
) -> io::Result<()> {
    unsafe {
        let res = libc::setsockopt(
            sock.as_raw_fd(),
            level,
            option_name,
            (&raw const option_value) as *const _,
            size_of::<T>() as socklen_t,
        );
        if res < 0{
            Err(io::Error::last_os_error())
        }else{
            Ok(())
        }
    }
}

impl<T: AsRawFd> SSM for T{
    fn join_ssm_multicast_v4(&self, group: &Ipv4Addr, itf: &Ipv4Addr, source: &Ipv4Addr) -> io::Result<()>{
        let mreq = ip_mreq_source {
            imr_multiaddr: ip_v4_addr_to_c(group),
            imr_interface: ip_v4_addr_to_c(itf),
            imr_sourceaddr: ip_v4_addr_to_c(source)
        };
        setsockopt(self, IPPROTO_IP, IP_ADD_SOURCE_MEMBERSHIP, mreq)
    }

    fn leave_ssm_multicast_v4(&self, group: &Ipv4Addr, itf: &Ipv4Addr, source: &Ipv4Addr) -> io::Result<()>{
        let mreq = ip_mreq_source {
            imr_multiaddr: ip_v4_addr_to_c(group),
            imr_interface: ip_v4_addr_to_c(itf),
            imr_sourceaddr: ip_v4_addr_to_c(source)
        };
        setsockopt(self, IPPROTO_IP, IP_DROP_SOURCE_MEMBERSHIP, mreq)
    }
}
