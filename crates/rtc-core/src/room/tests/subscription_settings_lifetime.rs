//! Public Room API/settings effects; explicit media receipts are model controls, not wire proof.
use super::*;
struct Fixture {
    r: Room,
    at: Instant,
    b: Joined,
    track: TrackSid,
}
fn fixture() -> Fixture {
    let (mut r, at) = room();
    let a = r.join(params("publisher", 17, true), at).unwrap();
    let effects = r.take_effects();
    let t = transport_of(&effects, &a.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: a.sid.clone(),
            transport: t,
        },
        at,
    );
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::AddTrack(add_track(
            "audio",
            TrackKind::Audio,
            TrackSource::Microphone,
        )),
        at,
    );
    r.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: a.sid.clone(),
            transport: t,
            mid: "a".into(),
            kind: TrackKind::Audio,
            msid_track_id: "audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        at,
    );
    let track = r.participants[&a.sid].info.tracks[0].sid.clone();
    r.take_effects();
    let mut bp = params("subscriber", 17, true);
    bp.auto_subscribe = false;
    let b = r.join(bp, at).unwrap();
    let effects = r.take_effects();
    let t = transport_of(&effects, &b.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: b.sid.clone(),
            transport: t,
        },
        at,
    );
    r.take_effects();
    Fixture { r, at, b, track }
}
impl Fixture {
    fn settings(&mut self, sec: u64) {
        self.r.handle_request(
            &self.b.sid,
            self.b.generation,
            SessionRequest::TrackSettings {
                track_sids: vec![self.track.clone()],
                settings: TrackSettings {
                    disabled: true,
                    ..Default::default()
                },
            },
            self.at + Duration::from_secs(sec),
        );
        self.r.take_effects();
    }
    fn tick(&mut self, sec: u64) {
        self.r.tick(self.at + Duration::from_secs(sec));
        self.r.take_effects();
    }
    fn sub(&mut self, wanted: bool, sec: u64) -> Vec<Effect> {
        self.r
            .api_update_subscriptions(
                &Identity::from("subscriber"),
                std::slice::from_ref(&self.track),
                wanted,
                self.at + Duration::from_secs(sec),
            )
            .unwrap();
        self.r.take_effects()
    }
}
fn admitted(effects: &[Effect]) -> Vec<bool> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Media(MediaCommand::Subscribe(s)) => Some(s.settings.disabled),
            _ => None,
        })
        .collect()
}
#[test]
fn settings_only_expiry_and_repeated_undesired_update_use_source_keep_until() {
    let mut f = fixture();
    f.settings(0);
    f.tick(59);
    assert_eq!(admitted(&f.sub(true, 59)), vec![true]);
    let mut f = fixture();
    f.settings(0);
    f.tick(59);
    f.tick(60);
    assert_eq!(
        admitted(&f.sub(true, 60)),
        vec![false],
        "expired settings cannot affect a later actual Subscribe spec"
    );
    let mut f = fixture();
    f.settings(0);
    f.settings(50);
    f.tick(60);
    assert_eq!(
        admitted(&f.sub(true, 60)),
        vec![true],
        "settings received while undesired renew the keep window"
    );
    let mut f = fixture();
    f.settings(0);
    f.settings(50);
    f.tick(111);
    assert_eq!(admitted(&f.sub(true, 111)), vec![false]);
}

mod lifecycle;
