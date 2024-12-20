use std::{io::{self, Error}, mem::{self, transmute, MaybeUninit}, net::{Ipv4Addr, SocketAddr}, os::{fd::AsRawFd, raw::c_void}};

use libc::{c_int, msghdr, socklen_t, CMSG_DATA, CMSG_FIRSTHDR, CMSG_NXTHDR, IPPROTO_IP, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, IPV6_RECVHOPLIMIT, IPV6_UNICAST_HOPS, IP_MULTICAST_TTL, IP_RECVTTL, IP_TTL};
use quiche::ancillaries::{Ancillary, ECNValue};
use socket2::{Domain, MaybeUninitSlice, MsgHdrMut, SockAddr, Socket, Type};

#[derive(Debug)]
pub struct MsgSocket{
    pub socket: Socket,
    pub multicast: bool,
}

impl MsgSocket{
    pub fn new(addr: SocketAddr, multicast: bool) -> Self{
        let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
        let socket = socket2::Socket::new(domain, Type::DGRAM, None).unwrap();
        socket.set_nonblocking(true).unwrap();
        socket.bind(&addr.into()).unwrap();

        MsgSocket{socket, multicast}
    }

    fn ancillaries(msg: MsgHdrMut<'_, '_, '_>) -> Vec<Ancillary>{
        let mut ancillaries = Vec::new();
        unsafe {
            let msg_pointer: *const MsgHdrMut = &msg;
            let msg_pointer = msg_pointer as *const msghdr;
            let mut control_msg = CMSG_FIRSTHDR(msg_pointer);

            while !control_msg.is_null(){
                let cmsg_level = (*control_msg).cmsg_level;
                let cmsg_type = (*control_msg).cmsg_type;
                let data = CMSG_DATA(control_msg);

                match Ancillary::from_raw(cmsg_level, cmsg_type, data) {
                    Ok(ancillary) => ancillaries.push(ancillary),
                    Err(err) => println!("{}", err)
                }

                control_msg = CMSG_NXTHDR(msg_pointer, control_msg);
            }
        }
        ancillaries
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr, Vec<Ancillary>), Error>{
        let buf: &mut [MaybeUninit<u8>] = unsafe { transmute(buf) };
        let mut control= Vec::new();
        control.resize(64000, MaybeUninit::zeroed());

        let addr_storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let len = mem::size_of_val(&addr_storage) as libc::socklen_t;
        let mut from = unsafe{ SockAddr::new(addr_storage, len) };
        let mut buffers = [MaybeUninitSlice::new(buf)];
        let mut msg = MsgHdrMut::new().with_buffers(&mut buffers).with_control(&mut control).with_addr(&mut from);
        let received = self.socket.recvmsg(&mut msg, 0);

        match received {
            Ok(n) => {
                let ancillaries = Self::ancillaries(msg);

                Ok((n, from.as_socket().unwrap(), ancillaries))
            },
            Err(err) => Err(err)
        }
    }

    pub fn send_to(&self, buf: &[u8], addr: &SocketAddr) -> io::Result<usize> {
        let addr: SockAddr = (*addr).into();
        self.socket.send_to(buf, &addr)
    }

    pub fn local_addr(&self) -> Result<SocketAddr, Error>{
        self.socket.local_addr().map(|addr| addr.as_socket().unwrap())
    }

    pub fn join_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> io::Result<()>{
        self.socket.join_multicast_v4(multiaddr, interface)
    }

    pub fn leave_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<(), Error>{
        self.socket.leave_multicast_v4(multiaddr, interface)
    }

    pub fn setsockopt(&self, level: c_int, name: c_int, value: *const c_void, option_len: socklen_t) -> c_int{
        unsafe{
            libc::setsockopt(self.socket.as_raw_fd(), level, name, value, option_len)
        }
    }

    pub fn recv_ttl(&self) -> io::Result<()>{
        let value: *const bool = &true;
        let res = match self.socket.local_addr().unwrap().is_ipv4() {
            true => self.setsockopt(IPPROTO_IP, IP_RECVTTL, value as *const c_void, mem::size_of::<bool>() as u32),
            false => self.setsockopt(IPPROTO_IPV6, IPV6_RECVHOPLIMIT, value as *const c_void, mem::size_of::<u32>() as u32),
        };

        if res == 0{
            Ok(())
        }else{
            io::Result::Err(io::Error::new(io::ErrorKind::Unsupported, "Failed to set the socket option"))
        }
    }

    pub fn set_ttl(&self, value: u32) -> io::Result<()>{
        let value: *const u32 = &value;
        let res = match self.socket.local_addr().unwrap().is_ipv4() {
            true => {
                let name = if self.multicast { IP_MULTICAST_TTL } else { IP_TTL };
                self.setsockopt(IPPROTO_IP, name, value as *const c_void, mem::size_of::<i32>() as u32)
            },
            false => {
                let name = if self.multicast { IPV6_MULTICAST_HOPS } else { IPV6_UNICAST_HOPS };
                self.setsockopt(IPPROTO_IPV6, name, value as *const c_void, mem::size_of::<i32>() as u32)
            },
        };
        if res == 0{
            Ok(())
        }else{
            io::Result::Err(io::Error::new(io::ErrorKind::Unsupported, "Failed to set the socket option"))
        }
    }

    pub fn recv_ecn(&self) -> io::Result<()>{
        match self.socket.local_addr().unwrap().is_ipv4(){
            true => self.socket.set_recv_tos(true),
            false => self.socket.set_recv_tclass_v6(true),
        }
    }

    pub fn set_ecn(&self, value: ECNValue) -> io::Result<()>{
        match self.socket.local_addr().unwrap().is_ipv4() {
            true => self.socket.set_tos(value.into()),
            false => self.socket.set_tclass_v6(value.into()),
        }
    }
}
