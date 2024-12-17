//! Receiving-side of the file transfer module.

use std::{fs, path::Path};
use std::io::Write;
use tokio::sync::mpsc::Receiver;
use super::Result;

#[derive(Debug)]
/// File transfer receiving-side specific messages.
pub enum FileTransferRecvMsg {
    /// Data.
    Data(Vec<u8>),

    /// No more data will be received.
    Close,
}

#[derive(Debug)]
/// Receiver structure to handle reception of file transfer.
pub struct FileTransferRecv {
    /// File to write data in.
    file: Option<fs::File>,

    /// Total number of bytes received so far.
    nb_bytes_recv: usize,

    /// Tokio channel to receive the data.
    rx_chan: Receiver<FileTransferRecvMsg>,
}

impl FileTransferRecv {
    /// New structure to handle the file transfer delivery on the receiving-side.
    pub fn new(filepath: &Path, rx_chan: Receiver<FileTransferRecvMsg>) -> Result<Self> {
        Ok(Self {
            file: Some(fs::File::create(filepath)?),
            nb_bytes_recv: 0,
            rx_chan,
        })
    }

    /// Runs the structure inside a tokio task.
    /// 
    /// It will get data from the channel and write them on disk.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            match self.rx_chan.recv().await {
                Some(FileTransferRecvMsg::Data(v)) => self.handle_new_data(v).await?,
                Some(FileTransferRecvMsg::Close) => {
                    self.rx_chan.close();
                    break;
                }
                None => break,
            }
        }

        Ok(())
    }

    pub async fn handle_new_data(&mut self, v: Vec<u8>) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.write_all(&v)?;
            self.nb_bytes_recv += v.len();
        }
        
        Ok(())
    }
}