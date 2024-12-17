//! Sending-side of the file transfer module.

use super::Result;
use std::fs;
use std::io::Read;
use std::path::Path;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
/// File transfer sending-side specific messages.
pub enum FileTransferSrcMsg {
    /// Data anw whether it is the last piece of data (i.e., is fin after).
    Data((Vec<u8>, bool)),

    /// No more data will be sent.
    Close,
}

#[derive(Debug)]
/// Sender structure to handle the emission of file transfer.
pub struct FileTransferSrc {
    /// File to read data from.
    file: fs::File,

    /// Length of the file at the time it is opened.
    len: u64,

    /// Total number of bytes sent so far.
    nb_bytes_sent: u64,

    /// Tokio channel to send the data.
    tx_chan: Sender<FileTransferSrcMsg>,
}

impl FileTransferSrc {
    /// New structure to handle the file transfer delivery on the sending-side.
    pub fn new(filepath: &Path, tx_chan: Sender<FileTransferSrcMsg>) -> Result<Self> {
        let file = fs::File::open(filepath)?;
        let len = file.metadata()?.len();
        Ok(Self {
            file,
            len,
            nb_bytes_sent: 0,
            tx_chan,
        })
    }

    /// Runs the structure inside a tokio task.
    ///
    /// It will get the data from the file and send them on the channel.
    ///
    /// Because the channel should be bounded, this task will block when the
    /// channel is full with some piece of waiting data.
    pub async fn run(&mut self) -> Result<()> {
        let mut buffer = vec![0u8; 2000];
        loop {
            let nb_read = self.file.read(&mut buffer)?;

            if nb_read == 0 {
                self.on_finish().await?;
                break;
            } else {
                self.nb_bytes_sent += nb_read as u64;
                let fin = self.nb_bytes_sent == self.len;
                let msg = FileTransferSrcMsg::Data((buffer, fin));
                self.tx_chan.send(msg).await?;
                buffer = vec![0u8; 2000];
            }
        }

        Ok(())
    }

    /// Call this function when the file is entirely read.
    /// This will close the sending side of the channel.
    /// 
    /// No verification is performed to know if the file is really entirely read.
    pub async fn on_finish(&mut self) -> Result<()> {
        let msg = FileTransferSrcMsg::Close;
        self.tx_chan.send(msg).await?;
        Ok(())
    }
}
