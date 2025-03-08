//! Kmeans congestion control algorithm
use crate::Connection;

use super::FcCongestionHeuristicOps;

/// Always join the group with closest rate
/// This suppose that the congestion window of a client reflects their
/// real rate, thus assuming that we rely on classical congestion control
/// provided by paths
pub static KMEANS: FcCongestionHeuristicOps = FcCongestionHeuristicOps {
    should_change_channel: kmeans_should_change_channel,
    did_change_channel: |_|{}
};

fn kmeans_should_change_channel(conn: &mut Connection) -> Option<Vec<u8>> {
    let flexicast = conn.flexicast.as_mut().unwrap();
    if let Some(recv_info) = &flexicast.congestion_state.received_congestion_info{
        let cwnd = recv_info.cwnd;
        let mut closest = recv_info.mc_cwnds.iter().next().unwrap();
        for group_info in recv_info.mc_cwnds.iter(){
            let (_, mc_cwnd) = group_info;
            if cwnd.abs_diff(*mc_cwnd) < cwnd.abs_diff(closest.1){
                closest = group_info;
            }
        }
        let channel_id = closest.0.clone();
        if flexicast.get_mc_announce_data_active().unwrap().channel_id == channel_id{
            return None;
        }
        return Some(channel_id);
    }
    None
}
