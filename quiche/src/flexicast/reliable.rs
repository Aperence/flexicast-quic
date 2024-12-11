//! Reliability management for Flexicast QUIC.
//! Depending on the role, the attributes are different.

use crate::packet::Epoch;
use crate::ranges::RangeSet;
use crate::recovery::flexicast::FcRecovery;
use crate::Connection;
use crate::Error;
use crate::Result;
use std::collections::HashSet;
use std::time;

use super::ack::McAck;
use super::FcError;
use super::McClientStatus;
use super::McRole;

#[derive(Debug, PartialEq, Eq, Default)]
/// Reliable flexicast attributes for the receiver.
pub struct RFcRecv {
    /// Next time the receiver will send a PATH_ACK, if ack delay is used.
    rfc_next_path_ack: Option<time::Instant>,

    /// Whether the receiver must send positive acknowledgment packets.
    rfc_recv_send_ack: bool,
}

impl RFcRecv {
    /// Sets the [`RFcRecv::rfc_recv_send_ack`].
    pub fn set_rfc_recv_send_ack(&mut self, v: bool) {
        self.rfc_recv_send_ack = v;
    }
}

#[derive(Debug)]
/// Reliable flexicast attributes for the unicast path source.
pub struct RFcUcPath {
    /// Multicast Ack aggregator.
    /// The role of this structure here is different than for the flexicast
    /// source. Here, the structure helps the unicast path to know which
    /// stream offsets have been delegated for unicast retransmission and which
    /// packets have been received to give to the flexicast flow aggregation
    /// of acks.
    pub(crate) mc_ack: McAck,

    /// Range of packet numbers that have been acknowledged by the receiver.
    /// Used for the delegation of streams.
    pub(crate) fc_pn_recv: RangeSet,

    /// Whether the flexicast flow is aware that this client listens to it.
    pub notified_fc_source: bool,

    /// Current highest packet number on the flexicast flow that this receiver
    /// instance sees.
    pub fc_highest_pn: Option<u64>,
}

impl Default for RFcUcPath {
    fn default() -> Self {
        let mut mc_ack = McAck::new();

        mc_ack.new_recv(0);

        Self {
            mc_ack,
            notified_fc_source: false,
            fc_highest_pn: None,
            fc_pn_recv: RangeSet::default(),
        }
    }
}

#[derive(Debug, Default)]
/// Reliable flexicast attributes for the flexicast flow.
pub struct RFcSource {
    /// Multicast acknowledgment aggregator.
    pub(crate) mc_ack: McAck,
}

/// Reliable flexicast attributes.
#[derive(Debug)]
pub enum ReliableFc {
    /// Receiver-specific.
    /// Used to store information about the next acks to send.
    Receiver(RFcRecv),

    /// Unicast-server specific reliable flexicast.
    /// Used to store the positive acks sent by the client about the flexicast
    /// channel.
    UcPath(RFcUcPath),

    /// Multicast source specific reliable flexicast.
    FcFlow(RFcSource),

    /// Undefined role. Used to initialise the structure at first.
    Undefined,
}

impl ReliableFc {
    /// Return a mutable reference to the client inner structure.
    pub fn receiver(&self) -> Option<&RFcRecv> {
        if let Self::Receiver(c) = self {
            Some(c)
        } else {
            None
        }
    }

    /// Return a mutable reference to the server inner structure.
    pub fn server(&self) -> Option<&RFcUcPath> {
        if let Self::UcPath(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Return a reference to the source inner structure.
    pub fn source(&self) -> Option<&RFcSource> {
        if let Self::FcFlow(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Return a mutable reference to the client inner structure.
    pub fn client_mut(&mut self) -> Option<&mut RFcRecv> {
        if let Self::Receiver(c) = self {
            Some(c)
        } else {
            None
        }
    }

    /// Return a mutable reference to the server inner structure.
    pub fn server_mut(&mut self) -> Option<&mut RFcUcPath> {
        if let Self::UcPath(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Return a mutable reference to the source inner structure.
    pub fn source_mut(&mut self) -> Option<&mut RFcSource> {
        if let Self::FcFlow(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

impl Connection {
    /// Gives ranges of received packets from the receivers to the flexicast
    /// flow. Internally calls [`crate::Connection::on_ack_received`].
    pub fn fc_on_ack_received(
        &mut self, ranges: &RangeSet, now: time::Instant,
    ) -> Result<()> {
        let hs = self.handshake_status();

        let fca = fca!(self)?;
        if !matches!(fca.mc_role, McRole::ServerFlexicast) {
            return Err(Error::Flexicast(FcError::McInvalidRole(fca.mc_role)));
        }
        let fc_path_id =
            fca.fc_path_id.ok_or(Error::Flexicast(FcError::McPath))?;

        if let Some(pid) = self.paths.pid_from_path_id(fc_path_id) {
            let is_app_limited = self.delivery_rate_check_if_app_limited(pid);
            let p = self.paths.get_mut(pid)?;
            if is_app_limited {
                p.recovery.delivery_rate_update_app_limited(true);
            }

            let (lost_pkt, lost_bytes, acked_bytes) =
                p.recovery.on_ack_received(
                    ranges,
                    0,
                    Epoch::Application,
                    hs,
                    now,
                    &self.trace_id,
                )?;

            self.lost_count += lost_pkt;
            self.lost_bytes += lost_bytes as u64;
            self.acked_bytes += acked_bytes as u64;

            // Drain packets from the McAck structure.
            let largest_pn = p.recovery.get_lowest_pn_app_epoch();
            if let Some(mc_ack) = self.get_mc_ack_mut() {
                mc_ack.drain_packets(largest_pn);
            }
        }

        Ok(())
    }

    /// Gives ranges of received stream pieces that have been delegated for
    /// unicast retransmission. These pieces of streams have been received
    /// by all receivers and can release memory from the flexicast flow.
    /// This basically copies the portion of code that is processed when a
    /// unicast server receives an ACK frame acknowledging a STREAM frame.
    pub fn fc_on_stream_ack_received(
        &mut self, stream_id: u64, off: u64, len: u64,
    ) -> Result<()> {
        let stream = self.streams.get_mut(stream_id);
        if let Some(stream) = stream {
            stream.send.ack_and_drop(off, len as usize);
            self.tx_buffered = self.tx_buffered.saturating_sub(len as usize);

            // Only collect the stream if it is complete and not
            // readable. If it is readable, it will get collected when
            // stream_recv() is used.
            if stream.is_complete() && !stream.is_readable() {
                let local = stream.local;
                self.streams.collect(stream_id, local);
            }
        } else {
            error!(
                "fc_on_stream_ack_received stream does not exist: {:?}",
                stream_id
            );
        }

        Ok(())
    }

    /// Sets the recovery mode of the flexicast flow.
    pub fn fc_set_recovery_state(&mut self) -> Result<()> {
        if let Some(flexicast) = self.flexicast.as_ref() {
            if flexicast.mc_role != McRole::ServerFlexicast {
                return Err(Error::Flexicast(FcError::McInvalidRole(
                    McRole::ServerFlexicast,
                )));
            }

            if let Some(path_id) = flexicast.get_fc_path_id() {
                let pid = self
                    .paths
                    .pid_from_path_id(path_id)
                    .ok_or(Error::Flexicast(FcError::McPath))?;
                let p = self.paths.get_mut(pid)?;
                p.recovery.fc_recovery = Some(FcRecovery::new(true));
                p.recovery.set_fc_recovery_epoch(true);
                return Ok(());
            }
            return Err(Error::Flexicast(FcError::McPath));
        }
        return Err(Error::Flexicast(FcError::McDisabled));
    }

    /// The flexicast flow delegates lost STREAM frames to the unicast paths.
    /// Receivers may receive multiple times the same STREAM frame if some
    /// packets get delayed.
    ///
    /// Requires that the caller is the flexicast flow and the callee the
    /// unicast path.
    ///
    /// The `retr_kind` indicates the type of retransmission that is performed.
    pub fn rfc_delegate_streams(
        &mut self, uc: &mut Connection, now: time::Instant,
        retr_kind: FcUnicastRetransmission,
    ) -> Result<()> {
        if self.flexicast.is_none() || uc.flexicast.is_none() {
            return Ok(());
        }

        let fc_fc = self.flexicast.as_mut().unwrap();
        let fc_uc = uc.flexicast.as_ref().unwrap();

        if !matches!(fc_fc.mc_role, McRole::ServerFlexicast) {
            return Err(Error::Flexicast(FcError::McInvalidRole(fc_fc.mc_role)));
        }

        if !matches!(
            fc_uc.mc_role,
            McRole::ServerUnicast(McClientStatus::ListenMcPath(true))
        ) {
            return Ok(());
        }

        // Delegate the streams on the unicast paths.
        let path_id =
            fc_fc.fc_path_id.ok_or(Error::Flexicast(FcError::McPath))?;
        let pid = self
            .paths
            .pid_from_path_id(path_id)
            .ok_or(Error::Flexicast(FcError::McPath))?;
        let path = self.paths.get_mut(pid)?;
        let streams_fc = &mut self.streams;
        let mc_ack = &mut self
            .flexicast
            .as_mut()
            .unwrap()
            .fc_reliable
            .source_mut()
            .unwrap()
            .mc_ack;
        let (_nb_lost_stream_frames, (lost_pn, recv_pn)) = path
            .recovery
            .delegate_streams(uc, streams_fc, mc_ack, retr_kind)?;

        let highest_pn =
            lost_pn.last().unwrap_or(0).max(recv_pn.last().unwrap_or(0));

        // Get the path on the unicast path.
        let pid = uc.paths.pid_from_path_id(path_id);
        if let Some(pid) = pid {
            if let Ok(path) = uc.paths.get_mut(pid) {
                path.recovery.set_largest_ack(highest_pn);
                let _out = path.recovery.detect_lost_packets(
                    Epoch::Application,
                    now,
                    &self.trace_id,
                );
            }
        }
        Ok(())
    }
}

/// All possible kinds of unicast retransmissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcUnicastRetransmission {
    /// The source retransmits reliable frames that it considers as lost after
    /// acknowledgment aggregation.
    /// 
    /// The boolean value indicates whether the flexicast flow can consider the packets delegated.
    Delegates(bool),

    /// Full retransmit. Happens when the source falls back on unicast for some
    /// receiver.
    FullRetransmit,

    /// This unicast path sees lost packets on its own and does not wait for the
    /// other receivers to receive the retransmissions. This is an
    /// optimization. The value contains the packet numbers that are lost
    /// for this receiver, using the QUIC reliability mechanism.
    PerUcPath(HashSet<u64>),
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use crate::flexicast::testing::*;

    impl FlexicastPipe {
        /// Same as `source_delegates_streams` but does not check for
        /// mc_timeout.
        pub fn source_delegates_streams_direct(
            &mut self, expired: time::Instant, mut retr_kind: FcUnicastRetransmission,
        ) -> Result<()> {
            let nb_recv = self.unicast_pipes.len();
            let ucs = self.unicast_pipes.iter_mut().take_while(|_| true);
            let mc = &mut self.mc_channel.channel;
            let ucs = ucs.map(|c| &mut c.0.server);

            ucs.enumerate().map(|(idx, uc)| {
                if matches!(retr_kind, FcUnicastRetransmission::Delegates(_)) {
                    if idx < nb_recv - 1 {
                        retr_kind = FcUnicastRetransmission::Delegates(false);
                    } else {
                        retr_kind = FcUnicastRetransmission::Delegates(true);
                    }
                }
                mc.rfc_delegate_streams(uc, expired, retr_kind.clone())
            })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flexicast::testing::*;
    use crate::flexicast::FcConfig;
    use crate::rand::rand_u8;

    impl FlexicastPipe {
        fn get_uc_path_mc_ack(&self, i: usize) -> Option<&McAck> {
            self.unicast_pipes
                .get(i)?
                .0
                .server
                .flexicast
                .as_ref()?
                .fc_reliable
                .server()
                .map(|s| &s.mc_ack)
        }
    }

    #[test]
    /// Tests that the received packets are correctly forwarded to the receiver
    /// and lie in the ReliableFc structure.
    fn test_fc_reliable_ack() {
        for probe_path in [false, true] {
            let mut fc_config = FcConfig {
                probe_mc_path: probe_path,
                ..Default::default()
            };

            let mut fc_pipe = FlexicastPipe::new(
                1,
                "/tmp/test_fc_reliable_ack",
                &mut fc_config,
            )
            .unwrap();

            assert!(fc_pipe.source_send_single_stream(true, None, 3).is_ok());
            assert!(fc_pipe.source_send_single_stream(true, None, 7).is_ok());
            let now = time::Instant::now();
            fc_pipe.server_control_to_mc_source(now).unwrap();

            let mut readables = fc_pipe.unicast_pipes[0]
                .0
                .client
                .readable()
                .collect::<Vec<_>>();
            readables.sort();
            assert_eq!(readables, vec![3, 7]);

            assert!(fc_pipe.clients_send().is_ok());

            let mc_ack = fc_pipe.get_uc_path_mc_ack(0).unwrap();
            let (_ack_pn, _streams, nb_recv) = mc_ack.get_state();
            assert_eq!(nb_recv, 1);
            let ack_pn = mc_ack.full_ack_poll().unwrap();
            let mut expected_ack_pn = RangeSet::default();
            expected_ack_pn.insert(2..4);
            assert_eq!(ack_pn, &expected_ack_pn);
        }
    }

    #[test]
    /// Tests the full reliability mechanism of flexicast using the McAck
    /// structure.
    fn test_fc_quic_reliability_with_mc_ack() {
        for probe_path in [false] {
            let mut fc_config = FcConfig {
                probe_mc_path: probe_path,
                ..Default::default()
            };
            let mut fc_pipe = FlexicastPipe::new(
                2,
                "/tmp/test_fc_quic_reliability_with_mc_ack",
                &mut fc_config,
            )
            .unwrap();

            let sleep_duration = time::Duration::from_millis(100);
            let now = time::Instant::now();

            // First stream is received by both receivers.
            let mc_ack = fc_pipe.mc_channel.channel.get_mc_ack_mut().unwrap();
            let (_, _, nb) = mc_ack.get_state();
            assert_eq!(nb, 2);
            fc_pipe.source_send_single_stream(true, None, 1).unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();

            // The unicast paths have state for the new packets.
            let uc = &mut fc_pipe.unicast_pipes[0].0.server;
            let path = uc.paths.get(1).unwrap();
            let sent_pkt = path.recovery.get_sent_pkts();
            assert_eq!(sent_pkt[0].pkt_num, 2);

            // Clients read the stream.
            let mut buf = [0u8; 500];
            let client_0 = &mut fc_pipe.unicast_pipes[0].0.client;
            assert!(client_0.stream_readable(1));
            assert_eq!(client_0.stream_recv(1, &mut buf), Ok((300, true)));
            let client_1 = &mut fc_pipe.unicast_pipes[1].0.client;
            assert!(client_1.stream_readable(1));
            assert_eq!(client_1.stream_recv(1, &mut buf), Ok((300, true)));

            // Flexicast source has a packet in waiting for ack.
            let fc = &mut fc_pipe.mc_channel.channel;
            let path = fc.paths.get(1).unwrap();
            let sent_pkt = path.recovery.get_sent_pkts();
            assert_eq!(sent_pkt[0].pkt_num, 1);
            assert_eq!(sent_pkt[1].pkt_num, 2);
            assert!(sent_pkt[1].time_acked.is_none());
            let nb_ack = fc_pipe.mc_channel.channel.acked_bytes;

            // The flexicast source acknowledged the packet because both receivers
            // said it was ok.
            std::thread::sleep(sleep_duration);
            fc_pipe.mc_channel.channel.on_timeout();
            let now = time::Instant::now();
            fc_pipe.clients_send().unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();

            // The flexicast flow and unicast path have acknowledged packets.
            let fc = &mut fc_pipe.mc_channel.channel;
            let path = fc.paths.get(1).unwrap();
            let sent_pkt = path.recovery.get_sent_pkts();
            assert!(sent_pkt[1].time_acked.is_some());

            let uc = &mut fc_pipe.unicast_pipes[0].0.server;
            let path = uc.paths.get(1).unwrap();
            assert!(fc_pipe.mc_channel.channel.acked_bytes > nb_ack);

            // McAck state is empty.
            let mc_ack = fc_pipe.mc_channel.channel.get_mc_ack_mut().unwrap();
            let (pns, ..) = mc_ack.get_state();
            assert_eq!(pns.len(), 0);

            // Second stream is lost for the first client.
            let mut client_losses = RangeSet::default();
            client_losses.insert(0..1);
            fc_pipe
                .source_send_single_stream(true, Some(&client_losses), 7)
                .unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();

            // Only second client receives data.
            let client_0 = &mut fc_pipe.unicast_pipes[0].0.client;
            assert!(!client_0.stream_readable(7));
            let client_1 = &mut fc_pipe.unicast_pipes[1].0.client;
            assert!(client_1.stream_readable(7));
            assert_eq!(client_1.stream_recv(7, &mut buf), Ok((300, true)));

            std::thread::sleep(sleep_duration);
            fc_pipe.mc_channel.channel.on_timeout();
            let now = time::Instant::now();
            fc_pipe.clients_send().unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();

            // No new complete acked packet.
            assert!(fc_pipe.mc_channel.channel.acked_bytes > nb_ack);

            // McAck contains state for this packet because it is not fully acked.
            let mc_ack = fc_pipe.mc_channel.channel.get_mc_ack_mut().unwrap();
            let (pns, streams, _) = mc_ack.get_state();
            assert_eq!(pns.len(), 1);
            assert_eq!(*pns.values().next().unwrap(), 1); // Only one client need to ack the packet.
            assert_eq!(streams.len(), 0);

            // The arrival of a new stream will trigger a loss for Stream 7.
            let now = time::Instant::now();
            fc_pipe.source_send_single_stream(true, None, 11).unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();
            std::thread::sleep(sleep_duration);
            let now = time::Instant::now();
            fc_pipe.clients_send().unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();
            fc_pipe
                .source_delegates_streams_direct(
                    now,
                    FcUnicastRetransmission::Delegates(true),
                )
                .unwrap();

            // The unicast server now has state for the expired streams.
            let open_stream_ids = fc_pipe.unicast_pipes[0]
                .0
                .server
                .streams
                .writable()
                .collect::<Vec<_>>();
            assert_eq!(open_stream_ids, vec![7]);

            assert!(!fc_pipe.mc_channel.channel.streams.is_collected(7));

            // And the McAck of both the flexicast source and the unicast server
            // have state.
            let mc_ack = fc_pipe.mc_channel.channel.get_mc_ack_mut().unwrap();
            let (_, streams, _) = mc_ack.get_state();
            assert_eq!(streams.len(), 1);
            let value = streams.get(&7).unwrap();
            assert_eq!(value.len(), 1);
            assert_eq!(value.iter().next().unwrap(), (&0, &(300, 1)));

            let mc_ack = &fc_pipe.unicast_pipes[0]
                .0
                .server
                .flexicast
                .as_ref()
                .unwrap()
                .fc_reliable
                .server()
                .unwrap()
                .mc_ack;
            let (_, streams, _) = mc_ack.get_state();
            assert_eq!(streams.len(), 1);
            let value = streams.get(&7).unwrap();
            assert_eq!(value.len(), 1);
            assert_eq!(value.iter().next().unwrap(), (&0, &(300, 1)));

            fc_pipe.unicast_pipes[0].0.advance().unwrap();

            // Client received the stream. State updated on the McAck of the
            // server.
            let mc_ack = &fc_pipe.unicast_pipes[0]
                .0
                .server
                .flexicast
                .as_ref()
                .unwrap()
                .fc_reliable
                .server()
                .unwrap()
                .mc_ack;
            let (_, streams, _) = mc_ack.get_state();
            assert!(streams.is_empty());

            fc_pipe.server_control_to_mc_source(now).unwrap();

            // Now the flexicast source does not have any state for the open
            // stream.
            let mc_ack = fc_pipe.mc_channel.channel.get_mc_ack_mut().unwrap();
            let (_, streams, _) = mc_ack.get_state();
            assert!(streams.is_empty());
            assert!(fc_pipe.mc_channel.channel.streams.is_collected(7));

            // First client now has the second stream.
            let client_0 = &mut fc_pipe.unicast_pipes[0].0.client;
            assert!(client_0.stream_readable(7));
            assert_eq!(client_0.stream_recv(7, &mut buf), Ok((300, true)));
        }
    }

    #[test]
    /// Tests the reliability mechanism of Flexicast QUIC with random packet
    /// losses.
    fn test_fc_quic_reliability_short_streams() {
        let mut fc_config = FcConfig {
            probe_mc_path: true,
            ..Default::default()
        };
        let mut fc_pipe = FlexicastPipe::new(
            2,
            "/tmp/test_fc_quic_reliability_short_streams",
            &mut fc_config,
        )
        .unwrap();

        let sleep_duration = time::Duration::from_millis(2);

        // Send multiple short streams that can lie in a single packet.
        let nb_streams = 1000;
        for i in 0..nb_streams {
            // Generate random losses.
            let mask = rand_u8();

            // Do not generate losses for the last 2 streams to ensure that we see
            // gaps.
            let client_loss = if mask & 0b1 > 0 && i < nb_streams - 2 {
                let mut losses = RangeSet::default();
                for j in 1..4 {
                    if mask & 1 << j > 0 {
                        losses.insert(j - 1..j);
                    }
                }
                Some(losses)
            } else {
                None
            };

            // The source sends the stream.
            let now = time::Instant::now();
            fc_pipe
                .source_send_single_stream(true, client_loss.as_ref(), 3 + i * 4)
                .unwrap();

            // The source notifies the unicast instances of the sent packet.
            fc_pipe.server_control_to_mc_source(now).unwrap();

            // Wait a bit...
            std::thread::sleep(sleep_duration);
            let now = time::Instant::now();

            // Clients send their feedback to the source.
            fc_pipe.clients_send().unwrap();
            fc_pipe.server_control_to_mc_source(now).unwrap();

            // Stream deleguation.
            fc_pipe
                .source_delegates_streams_direct(
                    now,
                    FcUnicastRetransmission::Delegates(true),
                )
                .unwrap();

            // Potentially unicast retransmissions.
            fc_pipe
                .unicast_pipes
                .iter_mut()
                .for_each(|(pipe, ..)| pipe.advance().unwrap());
        }

        // Ensure that each client received all the streams.
        for (pipe, ..) in fc_pipe.unicast_pipes.iter_mut() {
            let client = &mut pipe.client;
            for i in 0..nb_streams {
                assert!(client.stream_readable(3 + i * 4));
            }
        }
    }
}
