use tokio::{net::UdpSocket, sync::mpsc::{self, Receiver, Sender}};
use crate::fc_app::rtp::BufType;

use super::rtp::RtpServer;

struct RtpAsync{
    number_receivers: usize,
    server: RtpServer
}

impl RtpAsync{
    pub fn new(server: RtpServer, number_receivers: usize) -> Self{
        Self { server, number_receivers }
    }

    async fn process_control_message(&mut self, msg: ControlMsg, tx: &mut Sender<ControlMsgReply>){
        match msg{
            ControlMsg::HasData => {
                tx.send(ControlMsgReply::HasData(self.server.should_send_app_data())).await.unwrap();
            },
            ControlMsg::GetData => {
                let data = self.server.get_app_data();
                tx.send(ControlMsgReply::GetData(data)).await.unwrap();
            },
            ControlMsg::SentData(sent) => {
                self.server.stream_written(sent);
            },
            ControlMsg::Receivers(n) => {
                self.number_receivers = n;
            },
        }
    }

    async fn run_loop(mut self, mut tx: Sender<ControlMsgReply>, mut rx: Receiver<ControlMsg>){
        let mut buf = [0u8; 1600];
        loop {
            tokio::select! {
                data = rx.recv() => {
                    match data{
                        Some(data) => self.process_control_message(data, &mut tx).await,
                        None => break, // main thread quitted
                    }
                    println!("Received command")
                }
                data = self.server.additional_udp_socket_tokio().unwrap().recv(&mut buf) => {
                    if self.number_receivers == 0{
                        continue;
                    }
                    match data{
                        Ok(n) => {
                            self.server.handle_new_rtp_frame(BufType::Size(n), self.server.next_stream_id);
                            self.server.next_stream_id += 4;
                        },
                        Err(err) => {
                            println!("Error during recv: {}", err)
                        },
                    }

                }
            };
        }
    }

    pub fn run(mut self) -> RtpAsyncHandler{
        let (tx, rx) = mpsc::channel(1024);
        let (tx2, rx2) = mpsc::channel(1024);
        tokio::spawn(async move {
            self.run_loop(tx, rx2).await;
        });
        RtpAsyncHandler { receiver: rx, sender: tx2 }
    }
}

struct RtpAsyncHandler{
    sender: Sender<ControlMsg>,
    receiver: Receiver<ControlMsgReply>
}

impl RtpAsyncHandler{
    pub async fn has_data(&mut self) -> bool{
        self.sender.send(ControlMsg::HasData).await.unwrap();
        let reply = self.receiver.recv().await.unwrap();

        match reply{
            ControlMsgReply::HasData(status) => status,
            ControlMsgReply::GetData(_) => unreachable!(),
        }
    }

    pub async fn get_data(&mut self) -> Option<(u64, Vec<u8>)>{
        self.sender.send(ControlMsg::GetData).await.unwrap();
        let reply = self.receiver.recv().await.unwrap();

        match reply{
            ControlMsgReply::HasData(_) => unreachable!(),
            ControlMsgReply::GetData(data) => data,
        }
    }

    pub async fn sent_data(&mut self, len: usize){
        self.sender.send(ControlMsg::SentData(len)).await.unwrap();
    }

    pub async fn number_receivers(&mut self, n: usize){
        self.sender.send(ControlMsg::Receivers(n)).await.unwrap();
    }
}

enum ControlMsg{
    HasData,
    GetData,
    SentData(usize),
    Receivers(usize)
}

enum ControlMsgReply{
    HasData(bool),
    GetData(Option<(u64, Vec<u8>)>),
}