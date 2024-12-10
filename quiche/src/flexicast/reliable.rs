//! Reliability management for Flexicast QUIC.
//! Depending on the role, the attributes are different.

use crate::ranges::RangeSet;
use std::collections::HashSet;
use std::time;

use super::ack::McAck;

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
    /// stream offsets have been deleguated for unicast retransmission.
    pub(crate) mc_ack: McAck,

    /// Whether the flexicast flow is aware that this client listens to it.
    pub notified_fc_source: bool,
}

impl Default for RFcUcPath {
    fn default() -> Self {
        let mut mc_ack = McAck::new();

        mc_ack.new_recv(0);

        Self {
            mc_ack,
            notified_fc_source: false,
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

    /// Unicast-server specific reliable multicast.
    /// Used to store the positive acks sent by the client about the multicast
    /// channel.
    UcPath(RFcUcPath),

    /// Multicast source specific reliable multicast.
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

/// All possible kinds of unicast retransmissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcUnicastRetransmission {
    /// The source retransmits reliable frames that it considers as lost after
    /// acknowledgment aggregation.
    Delegates,

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
mod tests {
    use super::*;
    use crate::flexicast::testing::*;
    use crate::flexicast::FcConfig;

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
        let mut fc_config = FcConfig {
            probe_mc_path: false,
            ..Default::default()
        };

        let mut fc_pipe =
            FlexicastPipe::new(1, "/tmp/test_rfc_ack", &mut fc_config).unwrap();

        assert!(fc_pipe.source_send_single_stream(true, None, 3).is_ok());
        assert!(fc_pipe.source_send_single_stream(true, None, 7).is_ok());

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
