use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;
use shared::des::{EntityUpdate, RemoteDes};
use shared::{Destination, RemoteMessage};
use tangled::Reliability;

use super::messages::NetMsg;
use super::omni::OmniPeerId;
use super::world::WorldNetMessage;

const WINDOW: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(1);
const PING_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Counter {
    messages: u64,
    bytes: u64,
}

#[derive(Debug)]
struct Bucket {
    at: Instant,
    reliable: FxHashMap<&'static str, Counter>,
}

impl Bucket {
    fn new(at: Instant) -> Self {
        Self {
            at,
            reliable: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrafficSummary {
    pub(crate) category: &'static str,
    pub(crate) messages: u64,
    pub(crate) bytes: u64,
}

#[derive(Debug, Default)]
struct PeerDiagnostics {
    buckets: VecDeque<Bucket>,
    pending_ping: Option<PendingPing>,
    last_rtt: Option<Duration>,
    last_pong_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct PendingPing {
    nonce: u64,
    sent_at: Instant,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PingSummary {
    pub(crate) rtt: Option<Duration>,
    pub(crate) last_seen_ago: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct OutgoingDiagnostics {
    peers: std::sync::Mutex<FxHashMap<OmniPeerId, PeerDiagnostics>>,
    next_ping_nonce: AtomicU64,
}

impl Default for OutgoingDiagnostics {
    fn default() -> Self {
        Self {
            peers: Default::default(),
            next_ping_nonce: AtomicU64::new(1),
        }
    }
}

impl OutgoingDiagnostics {
    pub(crate) fn record(
        &self,
        peer: OmniPeerId,
        msg: &NetMsg,
        reliability: Reliability,
        encoded_len: usize,
    ) {
        self.record_at(Instant::now(), peer, msg, reliability, encoded_len)
    }

    pub(crate) fn record_at(
        &self,
        now: Instant,
        peer: OmniPeerId,
        msg: &NetMsg,
        reliability: Reliability,
        encoded_len: usize,
    ) {
        if reliability != Reliability::Reliable {
            return;
        }
        let mut peers = self.peers.lock().unwrap();
        let peer_stats = peers.entry(peer).or_default();
        prune_old(&mut peer_stats.buckets, now);
        let need_new = peer_stats
            .buckets
            .back()
            .is_none_or(|bucket| now.duration_since(bucket.at) >= Duration::from_secs(1));
        if need_new {
            peer_stats.buckets.push_back(Bucket::new(now));
        }
        let bucket = peer_stats.buckets.back_mut().unwrap();
        let counter = bucket.reliable.entry(classify_net_msg(msg)).or_default();
        counter.messages += 1;
        counter.bytes += encoded_len as u64;
    }

    pub(crate) fn top_reliable(
        &self,
        peer: OmniPeerId,
        now: Instant,
        limit: usize,
    ) -> Vec<TrafficSummary> {
        let mut peers = self.peers.lock().unwrap();
        let Some(peer_stats) = peers.get_mut(&peer) else {
            return Vec::new();
        };
        prune_old(&mut peer_stats.buckets, now);
        let mut totals: FxHashMap<&'static str, Counter> = Default::default();
        for bucket in &peer_stats.buckets {
            for (&category, counter) in &bucket.reliable {
                let total = totals.entry(category).or_default();
                total.messages += counter.messages;
                total.bytes += counter.bytes;
            }
        }
        let mut totals = totals
            .into_iter()
            .map(|(category, counter)| TrafficSummary {
                category,
                messages: counter.messages,
                bytes: counter.bytes,
            })
            .collect::<Vec<_>>();
        totals.sort_by_key(|summary| std::cmp::Reverse(summary.bytes));
        totals.truncate(limit);
        totals
    }

    pub(crate) fn begin_ping(&self, peer: OmniPeerId, now: Instant) -> Option<u64> {
        let mut peers = self.peers.lock().unwrap();
        let peer_stats = peers.entry(peer).or_default();
        if let Some(pending) = peer_stats.pending_ping
            && now.duration_since(pending.sent_at) < PING_INTERVAL
        {
            return None;
        }

        let nonce = self.next_ping_nonce.fetch_add(1, Ordering::Relaxed);
        peer_stats.pending_ping = Some(PendingPing {
            nonce,
            sent_at: now,
        });
        Some(nonce)
    }

    pub(crate) fn record_pong(&self, peer: OmniPeerId, nonce: u64, now: Instant) {
        let mut peers = self.peers.lock().unwrap();
        let Some(peer_stats) = peers.get_mut(&peer) else {
            return;
        };
        let Some(pending) = peer_stats.pending_ping else {
            return;
        };
        if pending.nonce != nonce {
            return;
        }

        peer_stats.last_rtt = Some(now.duration_since(pending.sent_at));
        peer_stats.last_pong_at = Some(now);
        peer_stats.pending_ping = None;
    }

    pub(crate) fn ping_summary(&self, peer: OmniPeerId, now: Instant) -> PingSummary {
        let peers = self.peers.lock().unwrap();
        let Some(peer_stats) = peers.get(&peer) else {
            return PingSummary::default();
        };
        PingSummary {
            rtt: peer_stats.last_rtt,
            last_seen_ago: peer_stats
                .last_pong_at
                .map(|last_pong_at| now.duration_since(last_pong_at)),
        }
    }

    pub(crate) fn ping_timeout() -> Duration {
        PING_TIMEOUT
    }
}

fn prune_old(buckets: &mut VecDeque<Bucket>, now: Instant) {
    while buckets
        .front()
        .is_some_and(|bucket| now.duration_since(bucket.at) > WINDOW)
    {
        buckets.pop_front();
    }
}

pub(crate) fn destinations_for(
    destination: &Destination<OmniPeerId>,
    host: OmniPeerId,
) -> Vec<OmniPeerId> {
    match destination {
        Destination::Peer(peer) => vec![*peer],
        Destination::Peers(peers) => peers.clone(),
        Destination::Host => vec![host],
        Destination::Broadcast => Vec::new(),
    }
}

pub(crate) fn classify_net_msg(msg: &NetMsg) -> &'static str {
    match msg {
        NetMsg::Welcome => "Welcome",
        NetMsg::RequestMods => "RequestMods",
        NetMsg::Mods { .. } => "Mods",
        NetMsg::EndRun => "EndRun",
        NetMsg::Kick => "Kick",
        NetMsg::Ping(_) => "Ping",
        NetMsg::Pong(_) => "Pong",
        NetMsg::PeerDisconnected { .. } => "PeerDisconnected",
        NetMsg::StartGame { .. } => "StartGame",
        NetMsg::ModRaw { .. } => "ModRaw",
        NetMsg::ModCompressed { .. } => "ModCompressed",
        NetMsg::WorldMessage(msg) => classify_world_msg(msg),
        NetMsg::PlayerColor(..) => "PlayerColor",
        NetMsg::RemoteMsg(msg) => classify_remote_msg(msg),
        NetMsg::ForwardDesToProxy(msg) => classify_des_to_proxy(msg),
        NetMsg::ForwardProxyToDes(msg) => classify_proxy_to_des(msg),
        NetMsg::ForwardProxyToWorldSync(_) => "ForwardProxyToWorldSync",
        NetMsg::NoitaDisconnected => "NoitaDisconnected",
        NetMsg::Flags(_) => "Flags",
        NetMsg::RespondFlagNormal(_, _) => "RespondFlagNormal",
        NetMsg::RespondFlagSlow(_, _) => "RespondFlagSlow",
        NetMsg::RespondFlagMoon(..) => "RespondFlagMoon",
        NetMsg::PlayerPosition(..) => "PlayerPosition",
        NetMsg::RespondFlagStevari(..) => "RespondFlagStevari",
        NetMsg::AudioData(..) => "AudioData",
        NetMsg::MapData(_) => "MapData",
        NetMsg::MatData(_) => "MatData",
    }
}

fn classify_remote_msg(msg: &RemoteMessage) -> &'static str {
    match msg {
        RemoteMessage::RemoteDes(des) => classify_remote_des(des),
    }
}

fn classify_remote_des(msg: &RemoteDes) -> &'static str {
    match msg {
        RemoteDes::Reset => "RemoteDes::Reset",
        RemoteDes::InterestRequest(_) => "RemoteDes::InterestRequest",
        RemoteDes::EntityUpdate(updates) => classify_entity_updates(updates),
        RemoteDes::EntityInit(_) => "RemoteDes::EntityInit",
        RemoteDes::ExitedInterest => "RemoteDes::ExitedInterest",
        RemoteDes::Projectiles(_) => "RemoteDes::Projectiles",
        RemoteDes::RequestGrab(_) => "RemoteDes::RequestGrab",
        RemoteDes::CameraPos(_) => "RemoteDes::CameraPos",
        RemoteDes::DeadEntities(_) => "RemoteDes::DeadEntities",
        RemoteDes::SpawnOnce(_, _) => "RemoteDes::SpawnOnce",
        RemoteDes::ChestOpen(..) => "RemoteDes::ChestOpen",
        RemoteDes::ChestOpenRequest(..) => "RemoteDes::ChestOpenRequest",
    }
}

fn classify_entity_updates(updates: &[EntityUpdate]) -> &'static str {
    if updates.iter().any(|update| {
        matches!(
            update,
            EntityUpdate::KillEntity { .. } | EntityUpdate::RemoveEntity(_)
        )
    }) {
        "RemoteDes::EntityDeath"
    } else {
        "RemoteDes::EntityUpdate"
    }
}

fn classify_des_to_proxy(msg: &shared::des::DesToProxy) -> &'static str {
    match msg {
        shared::des::DesToProxy::DeleteEntity(..) => "DesToProxy::DeleteEntity",
        shared::des::DesToProxy::ReleaseAuthority(_) => "DesToProxy::ReleaseAuthority",
        shared::des::DesToProxy::RequestAuthority { .. } => "DesToProxy::RequestAuthority",
        shared::des::DesToProxy::UpdatePosition(_) => "DesToProxy::UpdatePosition",
        shared::des::DesToProxy::UpdatePositions(_) => "DesToProxy::UpdatePositions",
        shared::des::DesToProxy::TransferAuthorityTo(..) => "DesToProxy::TransferAuthority",
        shared::des::DesToProxy::UpdateWand(..) => "DesToProxy::UpdateWand",
    }
}

fn classify_proxy_to_des(msg: &shared::des::ProxyToDes) -> &'static str {
    match msg {
        shared::des::ProxyToDes::GotAuthority(_) => "ProxyToDes::GotAuthority",
        shared::des::ProxyToDes::GotAuthoritys(_) => "ProxyToDes::GotAuthoritys",
        shared::des::ProxyToDes::RemoveEntities(_) => "ProxyToDes::RemoveEntities",
        shared::des::ProxyToDes::DeleteEntity(_) => "ProxyToDes::DeleteEntity",
    }
}

fn classify_world_msg(msg: &WorldNetMessage) -> &'static str {
    match msg {
        WorldNetMessage::RequestAuthority { .. } => "World::RequestAuthority",
        WorldNetMessage::AskForAuthority { .. } => "World::AskForAuthority",
        WorldNetMessage::GetChunk { .. } => "World::GetChunk",
        WorldNetMessage::LoseAuthority { .. } => "World::LoseAuthority",
        WorldNetMessage::ChangePriority { .. } => "World::ChangePriority",
        WorldNetMessage::GotAuthority { .. } => "World::GotAuthority",
        WorldNetMessage::RelinquishAuthority { .. } => "World::RelinquishAuthority",
        WorldNetMessage::UpdateStorage { .. } => "World::UpdateStorage",
        WorldNetMessage::AuthorityAlreadyTaken { .. } => "World::AuthorityAlreadyTaken",
        WorldNetMessage::ListenRequest { .. } => "World::ListenRequest",
        WorldNetMessage::ListenStopRequest { .. } => "World::ListenStopRequest",
        WorldNetMessage::UnloadChunk { .. } => "World::UnloadChunk",
        WorldNetMessage::ListenInitialResponse { .. } => "World::ListenInitialResponse",
        WorldNetMessage::ListenUpdate { .. } => "World::ListenUpdate",
        WorldNetMessage::ChunkPacket { .. } => "World::ChunkPacket",
        WorldNetMessage::ListenAuthorityRelinquished { .. } => "World::ListenAuthorityRelinquished",
        WorldNetMessage::GetAuthorityFrom { .. } => "World::GetAuthorityFrom",
        WorldNetMessage::RequestAuthorityTransfer { .. } => "World::RequestAuthorityTransfer",
        WorldNetMessage::TransferOk { .. } => "World::TransferOk",
        WorldNetMessage::TransferFailed { .. } => "World::TransferFailed",
        WorldNetMessage::NotifyNewAuthority { .. } => "World::NotifyNewAuthority",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::NetMsg;
    use crate::net::omni::OmniPeerId;
    use crate::net::world::WorldNetMessage;
    use shared::des::{EntityUpdate, RemoteDes};
    use shared::world_sync::{ChunkCoord, ProxyToWorldSync};
    use shared::{Destination, RemoteMessage};
    use std::time::{Duration, Instant};
    use tangled::Reliability;

    #[test]
    fn classifies_high_volume_latest_state_messages() {
        assert_eq!(classify_net_msg(&NetMsg::Ping(1)), "Ping");
        assert_eq!(classify_net_msg(&NetMsg::Pong(1)), "Pong");
        assert_eq!(
            classify_net_msg(&NetMsg::PlayerPosition(1, 2, false, true)),
            "PlayerPosition"
        );
        assert_eq!(
            classify_net_msg(&NetMsg::RemoteMsg(RemoteMessage::RemoteDes(
                RemoteDes::EntityUpdate(vec![EntityUpdate::SetHp(1.0)])
            ))),
            "RemoteDes::EntityUpdate"
        );
        assert_eq!(
            classify_net_msg(&NetMsg::WorldMessage(WorldNetMessage::ChunkPacket {
                chunkpacket: Vec::new()
            })),
            "World::ChunkPacket"
        );
    }

    #[test]
    fn classifies_large_initial_sync_messages() {
        assert_eq!(
            classify_net_msg(&NetMsg::MapData(Default::default())),
            "MapData"
        );
        assert_eq!(
            classify_net_msg(&NetMsg::MatData(Default::default())),
            "MatData"
        );
        assert_eq!(
            classify_net_msg(&NetMsg::ForwardProxyToWorldSync(ProxyToWorldSync::Updates(
                vec![]
            ))),
            "ForwardProxyToWorldSync"
        );
    }

    #[test]
    fn tracks_recent_reliable_bytes_per_peer() {
        let stats = OutgoingDiagnostics::default();
        let peer = OmniPeerId(7);
        let now = Instant::now();
        stats.record_at(
            now,
            peer,
            &NetMsg::PlayerPosition(1, 2, false, true),
            Reliability::Reliable,
            10,
        );
        stats.record_at(
            now + Duration::from_millis(10),
            peer,
            &NetMsg::MapData(Default::default()),
            Reliability::Unreliable,
            200,
        );
        stats.record_at(
            now + Duration::from_millis(20),
            peer,
            &NetMsg::MapData(Default::default()),
            Reliability::Reliable,
            30,
        );

        let top = stats.top_reliable(peer, now + Duration::from_millis(30), 4);
        assert_eq!(top[0].category, "MapData");
        assert_eq!(top[0].bytes, 30);
        assert_eq!(top[1].category, "PlayerPosition");
        assert_eq!(top[1].messages, 1);
    }

    #[test]
    fn expires_old_reliable_buckets() {
        let stats = OutgoingDiagnostics::default();
        let peer = OmniPeerId(9);
        let now = Instant::now();
        stats.record_at(
            now - Duration::from_secs(20),
            peer,
            &NetMsg::PlayerPosition(1, 2, false, true),
            Reliability::Reliable,
            10,
        );
        stats.record_at(
            now,
            peer,
            &NetMsg::MapData(Default::default()),
            Reliability::Reliable,
            30,
        );

        let top = stats.top_reliable(peer, now, 4);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].category, "MapData");
    }

    #[test]
    fn expands_destinations_for_peer_specific_accounting() {
        let peers = destinations_for(
            &Destination::Peers(vec![OmniPeerId(1), OmniPeerId(2)]),
            OmniPeerId(9),
        );
        assert_eq!(peers, vec![OmniPeerId(1), OmniPeerId(2)]);

        let host = destinations_for(&Destination::Host, OmniPeerId(9));
        assert_eq!(host, vec![OmniPeerId(9)]);
    }

    #[test]
    fn tracks_application_level_ping_round_trips() {
        let stats = OutgoingDiagnostics::default();
        let peer = OmniPeerId(7);
        let now = Instant::now();

        let nonce = stats.begin_ping(peer, now).unwrap();
        assert!(
            stats
                .begin_ping(peer, now + Duration::from_millis(100))
                .is_none()
        );

        stats.record_pong(peer, nonce + 1, now + Duration::from_millis(50));
        assert_eq!(
            stats.ping_summary(peer, now + Duration::from_millis(50)),
            PingSummary::default()
        );

        stats.record_pong(peer, nonce, now + Duration::from_millis(42));
        let summary = stats.ping_summary(peer, now + Duration::from_millis(100));
        assert_eq!(summary.rtt, Some(Duration::from_millis(42)));
        assert_eq!(summary.last_seen_ago, Some(Duration::from_millis(58)));
    }

    #[test]
    fn replaces_stale_pending_ping_after_interval() {
        let stats = OutgoingDiagnostics::default();
        let peer = OmniPeerId(8);
        let now = Instant::now();

        let first = stats.begin_ping(peer, now).unwrap();
        let second = stats
            .begin_ping(peer, now + Duration::from_secs(2))
            .unwrap();

        assert_ne!(first, second);
        stats.record_pong(
            peer,
            first,
            now + Duration::from_secs(2) + Duration::from_millis(30),
        );
        assert_eq!(
            stats.ping_summary(
                peer,
                now + Duration::from_secs(2) + Duration::from_millis(30)
            ),
            PingSummary::default()
        );

        stats.record_pong(
            peer,
            second,
            now + Duration::from_secs(2) + Duration::from_millis(40),
        );
        assert_eq!(
            stats
                .ping_summary(
                    peer,
                    now + Duration::from_secs(2) + Duration::from_millis(40)
                )
                .rtt,
            Some(Duration::from_millis(40))
        );
    }
}
