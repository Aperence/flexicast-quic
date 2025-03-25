use std::{collections::{HashMap, VecDeque}, convert::TryInto, net::SocketAddr, time::{Duration, Instant}};

use tokio::{fs::File, io::AsyncWriteExt, net::UdpSocket, sync::mpsc::{self, error::TryRecvError, Receiver, Sender}, task::JoinHandle};

use super::{pacer:: TokenPacer, rtp::RtpHeader};

type RtpPacket = Vec<u8>;

pub struct RtpAsync{
    socket: UdpSocket,
    queue: Sender<RtpPacket>,
    control: Receiver<ControlMsg>,
    number_receivers: usize,
    pacer: Option<TokenPacer>,
    packets: VecDeque<RtpPacket>,
    timestamp_count: HashMap<u32, usize>,
    frame_count: u64,
    logger: Option<File>
}

impl RtpAsync{
    pub async fn run(addr: SocketAddr, pacer: Option<TokenPacer>, logger: Option<String>) -> RtpAsyncHandler{
        let socket = UdpSocket::bind(addr).await.unwrap();

        let (queue_sender, queue_receiver) = mpsc::channel(1024);
        let (control_sender, control_receiver) = mpsc::channel(1024);

        let logger = match logger{
            Some(path) => {
                let mut file = File::create(path).await.expect("Failed to create logger");
                file.write("timestamp,frame_idx,number_packets\n".as_bytes()).await.expect("Failed to write header");
                Some(file)
            },
            None => None
        };

        let rtp = Self {
            socket,
            number_receivers: 0,
            queue: queue_sender,
            control: control_receiver,
            pacer,
            packets: VecDeque::new(),
            logger,
            timestamp_count: HashMap::new(),
            frame_count: 0,
        };

        let handle = tokio::spawn(async move {
            rtp.run_loop().await;
        });
        RtpAsyncHandler { handle, receiver: queue_receiver, sender: control_sender }
    }

    async fn handle_rtp(&mut self, rtp: &[u8]){
        debug!("rtp {}: received {} bytes", self.socket.local_addr().unwrap(), rtp.len());
        let rtp_header = RtpHeader::from_bytes(rtp[..12].try_into().unwrap());

        if rtp_header.payload_type == 96{
            let count = self.timestamp_count.entry(rtp_header.timestamp).or_insert(0);
            *count += 1;
            if rtp_header.marker{
                if let Some(logger) = &mut self.logger{
                    let log = format!("{},{},{}\n", rtp_header.timestamp, self.frame_count, *count);
                    logger.write(log.as_bytes())
                        .await
                        .expect("Failed to write to logger");
                }
                self.frame_count += 1;
                self.timestamp_count.remove(&rtp_header.timestamp);
            }
        }

        if self.number_receivers > 0{
            self.packets.push_back(rtp.to_vec());
        }
    }

    async fn handle_command(&mut self, command: ControlMsg){
        match command{
            ControlMsg::Receivers(n) => self.number_receivers = n,
        }
        debug!("rtp {}: set number of receivers to {}", self.socket.local_addr().unwrap(), self.number_receivers);
    }

    async fn send_packets(&mut self){
        loop{
            let now = Instant::now();
            let peek = self.packets.front();
            if peek.is_none(){
                return
            }
            let peek = peek.unwrap();
            let sent = match &mut self.pacer{
                Some(pacer) => pacer.send(peek.len(), now),
                None => true,
            };

            if sent{
                let packet = self.packets.pop_front().expect("Impossible");
                let res = self.queue.try_send(packet.clone());
                if res.is_err(){
                    // failed to send
                    self.packets.push_front(packet);
                    return;
                }
            }else{
                return; // come back later
            }
        }
    }

    async fn run_loop(mut self){
        let mut buf = [0u8; 1600];
        loop {
            tokio::select! {
                data = self.control.recv() => {
                    match data{
                        Some(data) => self.handle_command(data).await,
                        None => break, // main thread quitted
                    }
                    debug!("Rtp received command")
                }
                data = self.socket.recv(&mut buf) => {
                    match data{
                        Ok(n) => self.handle_rtp(&buf[..n]).await,
                        Err(err) => debug!("Error during recv: {}", err),
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    debug!("rtp {}: Timeout", self.socket.local_addr().unwrap());
                }
            };

            self.send_packets().await;
        }
    }
}

pub struct RtpAsyncHandler{
    handle: JoinHandle<()>,
    sender: Sender<ControlMsg>,
    receiver: Receiver<RtpPacket>
}

impl RtpAsyncHandler{
    pub async fn set_number_receivers(&mut self, n: usize){
        self.sender.send(ControlMsg::Receivers(n)).await.unwrap();
    }

    pub async fn recv(&mut self) -> Option<RtpPacket>{
        match self.receiver.try_recv(){
            Ok(data) => Some(data),
            Err(TryRecvError::Empty) => None,
            Err(_) => panic!("Failed to receive")
        }
    }
}

enum ControlMsg{
    Receivers(usize)
}
