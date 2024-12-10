use std::collections::HashSet;
use std::sync::Arc;

use crate::fca;
use crate::fca_mut;
use crate::flexicast::ack::McAck;
use crate::flexicast::reliable::FcUnicastRetransmission;
use crate::flexicast::FcError;
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
    pub(crate) is_fc_source: bool,

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
            is_fc_source,
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

        'per_packet: for packet in lost_iter {
            trace!(
                "Lost packet: {:?} with frames: {:?} and is lost={:?} is_acked={:?}", 
                packet.pkt_num, packet.frames, packet.time_lost.is_some(), packet.time_acked.is_some(),
            );

            // Indicate that this packet was delegated through unicast.
            if !matches!(retr_kind, FcUnicastRetransmission::PerUcPath(_)) {
                packet.is_fc_delegated = true;
            }

            // First check if the packet was received by the receiver.
            for r in recv_pn.iter() {
                let lowest_recovered_in_block = r.start;
                let largest_recovered_in_block = r.end - 1;
                if packet.pkt_num >= lowest_recovered_in_block &&
                    packet.pkt_num <= largest_recovered_in_block
                {
                    continue 'per_packet; // Packet was received.
                }
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
                        nb_lost_mc_stream_frames += 1;

                        trace!("Lost STREAM frame: ID={:?}, offset={:?}, length={:?}, fin={:?}", stream_id, offset, length, fin);

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
                            Err(Error::Done) if is_collected_on_uc => continue,
                            Err(e) => return Err(e),
                        };
                        let was_flushable_uc = stream_uc.is_flushable();

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

                        // Notify the multicast acknowledgment aggregator that we
                        // delegate a piece of stream.
                        if retr_kind == FcUnicastRetransmission::Delegates {
                            mc_ack.delegate(*stream_id, *offset, *length as u64);
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
                        let priority_key = Arc::clone(&stream_uc.priority_key);
                        if !was_flushable_uc {
                            uc.streams.insert_flushable(&priority_key);
                        }

                        // Notify the unicast instance that this piece of stream
                        // has been delegated by the flexicast source.
                        // Only notify if this is not a full retransmission,
                        // i.e., the flexicast source must not be aware that
                        // the stream was delegated since we entirely rely on
                        // unicast now (and the stream was
                        // not especially considered as lost for the source).
                        if retr_kind == FcUnicastRetransmission::Delegates {
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

        // Close existing streams on the unicast path if their are closed on the
        // flexicast flow. RFC-TODO: not optimal because we go over the
        // streams again. It should work by iterating over existing
        // finished streams were all data has already been sent. Not sure though.
        let expired_sent = self.epochs[Epoch::Application]
            .sent_packets
            .iter()
            .take_while(|p| p.time_lost.is_some() || p.time_acked.is_some())
            .filter(|p| p.time_lost.is_some());

        for pkt in expired_sent {
            for frame in pkt.frames.iter() {
                if let frame::Frame::StreamHeader {
                    stream_id,
                    offset,
                    length,
                    fin,
                } = frame
                {
                    if *fin {
                        // If the stream does not exist for the unicast path, it
                        // means that it did not have to
                        // retransmit frames to the receiver.
                        if let Some(stream_uc) = uc.streams.get_mut(*stream_id) {
                            stream_uc.send.fc_set_close_offset();
                            stream_uc
                                .send
                                .fc_set_fin_off(*offset + *length as u64);

                            // Maybe the stream is now complete.
                            if stream_uc.is_complete() && !stream_uc.is_readable()
                            {
                                let local = stream_uc.local;
                                uc.streams.collect(*stream_id, local);
                            }
                        }
                    }
                }
            }
        }

        // Reset the packet numbers that have been received.
        fca_mut!(uc)?.fc_reliable.server_mut().unwrap().fc_pn_recv =
            RangeSet::default();

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

    /// Sets the recovery epoch flexicast source.
    pub(crate) fn set_fc_recovery_epoch(&mut self, v: bool) {
        self.epochs.iter_mut().for_each(|e| e.is_fc_source = v);
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
