//! This module defines functions for data and control information exchange
//! between the flexicast source and the unicast instance servers. This module
//! intends to provide "multi-thread" friendly functions to exchange such
//! information.

use std::sync::Arc;
use std::time;

use super::reliable::FcUnicastRetransmission;
use super::FcError;
use super::McRole;
use crate::flexicast::ack::FcDelegatedStream;
use crate::flexicast::ack::McStreamOff;
use crate::packet::Epoch;
use crate::ranges::RangeSet;
use crate::recovery::Sent;
use crate::Connection;
use crate::Error;
use crate::Result;

/// Open version of the Recovery::Sent.
pub type OpenSent = Sent;

impl Connection {
    /// Sets the first packet number that the receiver must listen to.
    pub fn fc_set_first_pn(&mut self, pn: Option<u64>) {
        if let Some(fc) = self.flexicast.as_mut() {
            fc.fc_first_pn = pn;
        }
    }

    /// Returns the packet numbers that have been sent on the flexicast flow and
    /// that the receiver acknowledged. Also returns the stream ranges that
    /// had been delegated on the unicast path and have been acknowledged by the
    /// receiver. Returns an error if invalid role.
    ///
    /// Needs mutable.
    pub fn get_new_ack_pn_streams(
        &mut self,
    ) -> Result<(Option<RangeSet>, Option<McStreamOff>)> {
        if let Some(fc) = self.flexicast.as_mut() {
            if !matches!(fc.get_mc_role(), McRole::ServerUnicast(_)) {
                return Err(Error::Flexicast(FcError::McInvalidRole(
                    fc.get_mc_role(),
                )));
            }

            if let Some(rfc) = fc.fc_reliable.server_mut() {
                // Get newly acked packet numbers.
                let new_ack_pn = rfc.mc_ack.full_ack();

                // Get acked stream pieces.
                let ack_stream_pieces = rfc.mc_ack.acked_stream_off();

                return Ok((new_ack_pn, ack_stream_pieces));
            }
            return Err(Error::Flexicast(FcError::McReliableDisabled));
        }
        Err(Error::Flexicast(FcError::McDisabled))
    }

    /// Returns the set of packets that have been sent on the flexicast flow,
    /// since the last time this function was called and that are still in the
    /// sent queue of the flexicast source. Returns an error if this
    /// function is called with the wrong role.
    /// Also resets the RTT to the expiration timer.
    pub fn fc_get_sent_pkt(&mut self, from: Option<u64>) -> Result<Vec<Sent>> {
        if self.flexicast.is_none() {
            return Err(Error::Flexicast(FcError::McDisabled));
        }

        let flexicast = self.flexicast.as_mut().unwrap();
        if flexicast.get_mc_role() != McRole::ServerFlexicast {
            return Err(Error::Flexicast(FcError::McInvalidRole(
                McRole::ServerFlexicast,
            )));
        }

        let fc_path_id = flexicast
            .fc_path_id
            .ok_or(Error::Flexicast(FcError::McPath))?;
        let max_pn = from.unwrap_or(0);

        let path = self.paths.get_mut(fc_path_id as usize);
        if let Ok(path) = path {
            let (_new_max_pn, sent) =
                path.recovery.fc_get_sent_pkt(Epoch::Application, max_pn);
            if sent.is_empty() {
                return Err(Error::Done);
            }

            Ok(sent)
        } else {
            Err(Error::Flexicast(FcError::McPath))
        }
    }

    /// Notifies the connection of new packets that have been sent on the
    /// flexicast flow. Only available for the unicast server instances if
    /// the flexicast index is the correct one.
    pub fn fc_on_new_pkt_sent(
        &mut self, fc_id: usize, mut sent: Vec<Sent>,
    ) -> Result<()> {
        if self.flexicast.is_none() {
            return Err(Error::Flexicast(FcError::McDisabled));
        }

        let flexicast = self.flexicast.as_mut().unwrap();
        if !matches!(flexicast.get_mc_role(), McRole::ServerUnicast(_)) {
            return Err(Error::Flexicast(FcError::McInvalidRole(
                flexicast.get_mc_role(),
            )));
        }

        let frc = flexicast
            .fc_reliable
            .server_mut()
            .ok_or(Error::Flexicast(FcError::McReliableDisabled))?;

        let highest_pn = frc.fc_highest_pn;

        // Update the new highest packet number.
        if let Some(sent) = sent.last() {
            frc.fc_highest_pn = Some(sent.pkt_num + 1);
        }

        // Maybe during channel change we receive "old" sent packets. Avoid
        // putting them in our state.
        let joined_fc_id = flexicast.fc_chan_id.as_ref().map(|(_, id)| *id);
        if joined_fc_id != Some(fc_id) {
            return Ok(());
        }

        let fc_path_id = flexicast
            .get_fc_path_id()
            .ok_or(Error::Flexicast(FcError::McPath))?;

        let handshake_status = self.handshake_status();
        let trace_id = self.trace_id().to_string();
        let path = self.paths.get_mut(fc_path_id as usize)?;
        let now = time::Instant::now();

        for pkt in sent
            .drain(..)
            .filter(|s| s.pkt_num >= highest_pn.unwrap_or(0))
        {
            // Acknowledge the packet.
            path.recovery.on_packet_sent(
                pkt,
                Epoch::Application,
                handshake_status,
                now,
                &trace_id,
            );
        }

        Ok(())
    }

    /// Returns the set of STREAM frames that must be delegated to the receivers
    /// for unicast retransmission. This function does not take into account
    /// per-receiver reception of a STREAM frame, it will aggregate everything
    /// and forward all frames to the controller that will take the time to
    /// adjust to all clients.
    ///
    /// Returns an error if this is not the flexicast source.
    ///
    /// FC-TODO: this function does not take into account MC_ASYM frames for
    /// per-stream authentication! This will break per-stream authentication
    /// if the frame needs to be retransmitted.
    ///
    /// FC-TODO: also breaks FEC.
    ///
    /// The `early_retransmit` flag is set whenever the controller asks for
    /// the delegation of STREAM frames early in the process, i.e., frames that
    /// may not be lost will be delegated.
    pub fn fc_get_delegated_stream(
        &mut self, retr_kind: FcUnicastRetransmission,
    ) -> Result<Vec<FcDelegatedStream>> {
        if self.flexicast.is_none() {
            return Err(Error::Flexicast(FcError::McDisabled));
        }

        let flexicast = self.flexicast.as_ref().unwrap();
        if flexicast.get_mc_role() != McRole::ServerFlexicast {
            return Err(Error::Flexicast(FcError::McInvalidRole(
                McRole::ServerFlexicast,
            )));
        }

        let fc_path_id = flexicast
            .get_fc_path_id()
            .ok_or(Error::Flexicast(FcError::McPath))?;
        let fc_path = self.paths.get_mut(fc_path_id as usize)?;

        let streams = &mut self.streams;
        fc_path.recovery.fc_get_delegated_stream(streams, retr_kind)
    }

    /// Inserts in the unicast path delegated streams from the flexicast source.
    /// This creates states for streams that were previously sent on the
    /// flexicast flow and need unicast retransmission.
    ///
    /// Returns an error if this is not a unicast source instance.
    /// Does nothing if this is the wrong flexicast source ID, since we may be
    /// in a transient state because the receiver changed its flexicast flow.
    pub fn fc_delegated_streams(
        &mut self, fc_id: u64, mut delegated_streams: Vec<FcDelegatedStream>,
    ) -> Result<()> {
        let flexicast = self
            .flexicast
            .as_ref()
            .ok_or(Error::Flexicast(FcError::McDisabled))?;

        if !matches!(flexicast.get_mc_role(), McRole::ServerUnicast(_)) {
            return Err(Error::Flexicast(FcError::McInvalidRole(
                flexicast.get_mc_role(),
            )));
        }

        // Maybe a transient state.
        if flexicast
            .fc_chan_id
            .as_ref()
            .map(|(_, id)| *id as u64 != fc_id)
            .unwrap_or(true)
        {
            return Ok(());
        }

        for del_stream in delegated_streams.drain(..) {
            let is_stream_collected =
                self.streams.is_collected(del_stream.stream_id);
            // FC-TODO: Woops, won't work if not local stream!
            let stream =
                match self.get_or_create_stream(del_stream.stream_id, true) {
                    Ok(v) => v,
                    Err(Error::Done) if is_stream_collected => continue,
                    Err(e) => return Err(e),
                };

            let was_flushable = stream.is_flushable();

            debug!(
                "Client unicast retransmits stream piece because lost packet={}",
                del_stream.pn
            );

            // FC-TODO: stream rotation?

            let _written = match stream.send.write_at_offset(
                &del_stream.payload,
                del_stream.offset,
                del_stream.fin,
            ) {
                Ok(v) => v,
                Err(Error::FinalSize) => continue,
                Err(e) => return Err(e),
            };

            // Mark the stream as flushable.
            let priority_key = Arc::clone(&stream.priority_key);
            if !was_flushable {
                self.streams.insert_flushable(&priority_key);
            }
        }

        Ok(())
    }

    /// Notifies the unicast path that some streams have been collected on the flexicast flow.
    /// If this happens, the unicast path knows that it will not receive new unicast retransmissions
    /// and it can collect its stream once all data is acknowledged.
    pub fn fc_notify_collected_streams(&self, uc: &mut Connection) {
        let stream_ids = uc.streams.fc_get_stream_ids().map(|id| *id).collect::<Vec<_>>();
        for &stream_id in stream_ids.iter() {
            if self.streams.is_collected(stream_id) {
                if let Some(stream) = uc.streams.get_mut(stream_id) {
                    stream.send.fc_set_close_offset();
                    
                    // Maybe the stream is now complete.
                    if stream.is_complete() && !stream.is_readable()
                    {
                        let local = stream.local;
                        uc.streams.collect(stream_id, local);
                    }
                }
            }
        }
    }
}
