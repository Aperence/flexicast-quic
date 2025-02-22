use std::{convert::TryInto, net::UdpSocket};

use quiche_apps::fc_app::rtp::RtpHeader;

fn main(){
    let socket = UdpSocket::bind("0.0.0.0:8000").unwrap();

    let mut buffer = [0; 6400];
    let mut i = 0;
    while i < 250 {
        let _ = socket.recv(&mut buffer).unwrap();

        let header = RtpHeader::from_bytes(buffer[..12].try_into().unwrap());
        println!("Header: {:?}", header);
        i += 1;
    }
}
