use std::collections::HashSet;
use std::sync::Arc;

use crate::fca;
use crate::fca_mut;
use crate::flexicast::ack::FcDelegatedStream;
use crate::flexicast::ack::McAck;
use crate::flexicast::reliable::FcUnicastRetransmission;
use crate::flexicast::FcError;
use crate::flexicast::McRole;
use crate::frame;
use crate::packet::Epoch;
use crate::ranges::RangeSet;
use crate::stream::StreamMap;
use crate::Connection;
use crate::Error;
use crate::Result;

use super::Recovery;

/// Flexicast structure for the recovery mechanism of QUIC.
pub(crate) struct FcRecovery {
    /// Whether this is the recovery of the Flexicast QUIC source.
    pub(crate) _is_fc_source: bool,

    /// Flexicast.
    /// Packet numbers that have been newly acked.
    pub(crate) fc_new_ack_pn: Vec<u64>,

    /// Flexicast.
    /// Packet numbers that have been newly declared as lost.
    pub(crate) fc_new_lost_pn: Vec<u64>,
}

impl FcRecovery {
    /// New flexicast recovery.
    pub fn new(is_fc_source: bool) -> Self {
        Self {
            _is_fc_source: is_fc_source,
            fc_new_ack_pn: Vec::new(),
            fc_new_lost_pn: Vec::new(),
        }
    }
}

impl Recovery {
    /// Flexicast.
    /// Delegates the streams to another QUIC connection.
    ///
    /// Returns the number of lost STREAM frames that will be delegated.
    ///
    /// The `retr_kind` determines the strategy of delegation.
    pub fn delegate_streams(
        &mut self, uc: &mut Connection, local_streams: &mut StreamMap,
        mc_ack: &mut McAck, retr_kind: FcUnicastRetransmission,
    ) -> Result<(u64, (RangeSet, RangeSet))> {
        let recv_pn = fca!(uc)?
            .fc_reliable
            .server()
            .map(|rfc| rfc.fc_pn_recv.clone())
            .unwrap_or(RangeSet::default());

        let mut lost_pn = RangeSet::default();

        let recv_ack_rangeset = match retr_kind {
            FcUnicastRetransmission::PerUcPath(ref ranges) => {
                // If this is a per-receiver unicast path retransmission, we "ack"
                // it by saying that this receiver received the
                // packet, because the packet is not considered as
                // lost yet.
                if matches!(retr_kind, FcUnicastRetransmission::PerUcPath(_)) {
                    let mut r = RangeSet::default();
                    for pn in ranges.iter() {
                        r.insert(*pn..*pn + 1);
                    }
                    mc_ack.on_ack_received(&r);
                }
                ranges.clone()
            },
            _ => HashSet::new(),
        };

        let mut nb_lost_mc_stream_frames = 0;
        let lost_iter = self.epochs[Epoch::Application]
            .sent_packets
            .iter_mut()
            .take_while(|p| {
                p.time_lost.is_some() ||
                    p.time_acked.is_some() ||
                    retr_kind == FcUnicastRetransmission::FullRetransmit ||
                    !recv_ack_rangeset.is_empty()
            })
            .filter(|p| {
                p.time_lost.is_some() ||
                    retr_kind == FcUnicastRetransmission::FullRetransmit ||
                    recv_ack_rangeset.contains(&p.pkt_num)
            });

        let mut last_pkt_num = None;

        for packet in lost_iter {
            trace!(
                "Lost packet: {:?} with frames: {:?} and is lost={:?} is_acked={:?}", 
                packet.pkt_num, packet.frames, packet.time_lost.is_some(), packet.time_acked.is_some(),
            );

            // Indicate that this packet was delegated through unicast.
            // Only if we are sure that we can.
            if matches!(retr_kind, FcUnicastRetransmission::Delegates(true)) {
                packet.is_fc_delegated = true;
            }

            // First check if the packet was received by the receiver.
            let mut is_lost = true;
            for r in recv_pn.iter() {
                let lowest_recovered_in_block = r.start;
                let largest_recovered_in_block = r.end - 1;
                if packet.pkt_num >= lowest_recovered_in_block &&
                    packet.pkt_num <= largest_recovered_in_block
                {
                    is_lost = false;
                    break;
                }
            }

            if is_lost {
                lost_pn.insert(packet.pkt_num..packet.pkt_num + 1);

                for frame in packet.frames.iter() {
                    match frame {
                        frame::Frame::StreamHeader {
                            stream_id,
                            offset,
                            length,
                            fin,
                        } => {
                            nb_lost_mc_stream_frames += 1;

                            trace!("Lost STREAM frame: ID={:?}, offset={:?}, length={:?}, fin={:?} is_collected={:?} from pn={}", stream_id, offset, length, fin, local_streams.is_collected(*stream_id), packet.pkt_num);

                            // Get the stream on the flexicast flow.
                            let stream_fc = local_streams
                                .get_mut(*stream_id)
                                .ok_or(Error::InvalidStreamState(*stream_id))?;

                            let is_collected_on_uc =
                                uc.streams.is_collected(*stream_id);
                            let stream_uc = match uc
                                .get_or_create_stream(*stream_id, stream_fc.local)
                            {
                                Ok(v) => v,
                                Err(Error::Done) if is_collected_on_uc =>
                                    continue,
                                Err(e) => {
                                    return Err(e);
                                },
                            };
                            let was_flushable_uc = stream_uc.is_flushable();

                            // We "ack" the recovery mechanism by asking to
                            // retransmit
                            // the specified data... Since we
                            // call "send" on the data that is
                            // retransmitted, we assume that the call
                            // to "retransmit" wil be cancelled out.
                            stream_fc.send.retransmit(*offset, *length);

                            // ...and we get the data. This is not optimized (2
                            // copies) but requires the fewest
                            // changes.
                            let mut buf = vec![0u8; *length];
                            if let Err(Error::FinalSize) =
                                stream_fc.send.emit(&mut buf)
                            {
                                continue;
                            }

                            // Notify the multicast acknowledgment aggregator that
                            // we delegate a piece of
                            // stream.
                            if matches!(
                                retr_kind,
                                FcUnicastRetransmission::Delegates(_)
                            ) {
                                mc_ack.delegate(
                                    *stream_id,
                                    *offset,
                                    *length as u64,
                                );
                            }

                            let _written = match stream_uc.send.write_at_offset(
                                &buf[..],
                                *offset,
                                *fin,
                            ) {
                                Ok(v) => v,
                                Err(Error::FinalSize) => continue,
                                Err(e) => return Err(e),
                            };

                            // Mark the stream as flushable. We do not take into
                            // account flow limits because the
                            // data has already been sent once on
                            // the flexicast, and this data should be
                            // considered as a retransmission
                            // only.
                            let priority_key =
                                Arc::clone(&stream_uc.priority_key);
                            if !was_flushable_uc {
                                uc.streams.insert_flushable(&priority_key);
                            }

                            // Notify the unicast instance that this piece of
                            // stream
                            // has been delegated by the flexicast source.
                            // Only notify if this is not a full retransmission,
                            // i.e., the flexicast source must not be aware that
                            // the stream was delegated since we entirely rely on
                            // unicast now (and the stream was
                            // not especially considered as lost for the source).
                            if matches!(
                                retr_kind,
                                FcUnicastRetransmission::Delegates(_)
                            ) {
                                if let Some(rfc) =
                                    fca_mut!(uc)?.fc_reliable.server_mut()
                                {
                                    rfc.mc_ack.delegate(
                                        *stream_id,
                                        *offset,
                                        *length as u64,
                                    );
                                }
                            }
                        },

                        _ => (),
                    }
                }
            }

            // Drain the frames of the packet if it is lost.
            if matches!(retr_kind, FcUnicastRetransmission::Delegates(true)) {
                let _ = packet.frames.drain(..);
            }

            last_pkt_num = Some(packet.pkt_num);
        }

        // Reset the packet numbers that have been received.
        if let Some(last_pn) = last_pkt_num {
            fca_mut!(uc)?
                .fc_reliable
                .server_mut()
                .unwrap()
                .fc_pn_recv
                .remove_until(last_pn);
        }
        Ok((nb_lost_mc_stream_frames, (lost_pn, recv_pn)))
    }

    /// Returns the lowest packet number still in the sending queue of the
    /// Application Epoch on the provided space ID.
    pub fn get_lowest_pn_app_epoch(&self) -> Option<u64> {
        self.epochs[Epoch::Application]
            .sent_packets
            .iter()
            .map(|pkt| pkt.pkt_num)
            .next()
    }

    pub fn set_largest_ack(&mut self, largest: u64) {
        let pn = self.epochs[Epoch::Application].largest_acked_packet;
        if pn < Some(largest) {
            self.epochs[Epoch::Application].largest_acked_packet = Some(largest);
        }
    }

    /// Returns a copy of the packets sent.
    /// Only returns once each packet, and assumes that the caller already
    /// processed previously sent ones. Also returns the new paximum packet
    /// number.
    pub fn fc_get_sent_pkt(
        &self, epoch: Epoch, max_pn: u64,
    ) -> (u64, Vec<super::Sent>) {
        let new_max_pn = self.epochs[Epoch::Application]
            .sent_packets
            .back()
            .map(|s| s.pkt_num)
            .unwrap_or(0);

        let sent = self.epochs[epoch]
            .sent_packets
            .iter()
            .filter(|s| s.pkt_num >= max_pn)
            .map(|s| s.to_owned())
            .collect();
        (new_max_pn, sent)
    }

    /// Returns the set of STREAM frames that must be delegated to the receivers
    /// for unicast retransmission. This function does not take into account
    /// per-receiver reception of a STREAM frame, it will aggregate everything
    /// and forward all frames to the controller that will take the time to
    /// adjust to all receivers.
    pub fn fc_get_delegated_stream(
        &mut self, streams: &mut StreamMap, retr_kind: FcUnicastRetransmission,
    ) -> Result<Vec<FcDelegatedStream>> {
        let mut delegated_pieces = Vec::new();

        let mut lost_pn = RangeSet::default();

        let recv_ack_rangeset = match retr_kind {
            FcUnicastRetransmission::PerUcPath(ref ranges) => {
                // If this is a per-receiver unicast path retransmission, we "ack"
                // it by saying that this receiver received the
                // packet, because the packet is not considered as
                // lost yet.
                if matches!(retr_kind, FcUnicastRetransmission::PerUcPath(_)) {
                    let mut r = RangeSet::default();
                    for pn in ranges.iter() {
                        r.insert(*pn..*pn + 1);
                    }
                }
                ranges.clone()
            },
            _ => HashSet::new(),
        };

        let lost_iter = self.epochs[Epoch::Application]
            .sent_packets
            .iter_mut()
            .take_while(|p| {
                p.time_lost.is_some() ||
                    p.time_acked.is_some() ||
                    retr_kind == FcUnicastRetransmission::FullRetransmit ||
                    !recv_ack_rangeset.is_empty()
            })
            .filter(|p| {
                p.time_lost.is_some() ||
                    retr_kind == FcUnicastRetransmission::FullRetransmit ||
                    recv_ack_rangeset.contains(&p.pkt_num)
            });

        for packet in lost_iter {
            trace!(
                "Lost packet: {:?} with frames: {:?} and is lost={:?} is_acked={:?}", 
                packet.pkt_num, packet.frames, packet.time_lost.is_some(), packet.time_acked.is_some(),
            );

            // Indicate that this packet was delegated through unicast.
            // Only if we are sure that we can.
            if matches!(retr_kind, FcUnicastRetransmission::Delegates(true)) {
                packet.is_fc_delegated = true;
            }

            lost_pn.insert(packet.pkt_num..packet.pkt_num + 1);

            for frame in packet.frames.iter() {
                match frame {
                    frame::Frame::StreamHeader {
                        stream_id,
                        offset,
                        length,
                        fin,
                    } => {
                        trace!(
                            "Lost STREAM frame: ID={:?}, offset={:?}, length={:?}, fin={:?} is_collected={:?} from pn={}",
                            stream_id, offset, length, fin, streams.is_collected(*stream_id), packet.pkt_num
                        );

                        // Get the stream on the flexicast flow.
                        let stream_fc = streams
                            .get_mut(*stream_id)
                            .ok_or(Error::InvalidStreamState(*stream_id))?;

                        // We "ack" the recovery mechanism by asking to retransmit
                        // the specified data... Since we
                        // call "send" on the data that is
                        // retransmitted, we assume that the call
                        // to "retransmit" wil be cancelled out.
                        stream_fc.send.retransmit(*offset, *length);

                        // ...and we get the data. This is not optimized (2
                        // copies) but requires the fewest
                        // changes.
                        let mut buf = vec![0u8; *length];
                        if let Err(Error::FinalSize) =
                            stream_fc.send.emit(&mut buf)
                        {
                            continue;
                        }

                        delegated_pieces.push(FcDelegatedStream {
                            stream_id: *stream_id,
                            offset: *offset,
                            payload: buf,
                            pn: packet.pkt_num,
                            fin: *fin,
                        });
                    },

                    _ => (),
                }
            }

            // Drain the frames of the packet if it is lost.
            if matches!(retr_kind, FcUnicastRetransmission::Delegates(true)) {
                let _ = packet.frames.drain(..);
            }
        }

        Ok(delegated_pieces)
    }

    /// Sets the recovery epoch flexicast source.
    pub(crate) fn set_fc_recovery_epoch(&mut self, v: bool) {
        self.epochs.iter_mut().for_each(|e| e.is_fc_source = v);
    }

    /// Initiates flexicast state for the recovery.
    pub fn init_fc_recovery_state(&mut self, fc_role: McRole) {
        match fc_role {
            McRole::ServerFlexicast =>
                self.fc_recovery = Some(FcRecovery::new(true)),
            McRole::ServerUnicast(_) => {
                self.fc_recovery = Some(FcRecovery::new(false));
                self.epochs
                    .iter_mut()
                    .for_each(|e| e.fc_new_lost_pkt = Some(Vec::new()));
            },
            McRole::Client(_) => self.fc_recovery = Some(FcRecovery::new(false)),
            _ => (),
        }
    }

    /// Returns whether there are bytes in flight.
    pub fn bytes_in_flight(&self) -> bool {
        self.bytes_in_flight > 0
    }

    /// Forces the congestion window to a given value.
    ///
    /// We add the `fc` prefix to highlight the fact that this is not a standard
    /// quiche method, and hence it must be used carefully.
    pub fn fc_set_cwnd(&mut self, cwin: usize) {
        self.congestion.congestion_window = cwin;
    }
}

#[cfg(test)]
mod testing {
    use crate::recovery::Sent;

    use super::*;

    impl Recovery {
        pub fn get_sent_pkts(&self) -> Vec<Sent> {
            self.epochs[Epoch::Application]
                .sent_packets
                .iter()
                .cloned()
                .collect()
        }
    }
}
