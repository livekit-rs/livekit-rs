use super::*;
impl Fixture {
    fn closed(&mut self, sec: u64) -> Vec<Effect> {
        self.r.handle_media_event(
            MediaEvent::SubscriptionRemoved {
                subscriber: self.b.sid.clone(),
                track_sid: self.track.clone(),
            },
            self.at + Duration::from_secs(sec),
        );
        self.r.take_effects()
    }
}
#[test]
fn unsubscribe_does_not_start_a_sixty_second_ttl_and_waits_for_real_removal() {
    let mut f = fixture();
    f.settings(0);
    assert_eq!(admitted(&f.sub(true, 1)), vec![true]);
    f.r.handle_media_event(
        MediaEvent::SubscriptionPrepared {
            subscriber: f.b.sid.clone(),
            track_sid: f.track.clone(),
        },
        f.at + Duration::from_secs(1),
    );
    f.r.take_effects();
    let out = f.sub(false, 2);
    assert!(
        out.iter()
            .any(|e| matches!(e, Effect::Media(MediaCommand::Unsubscribe { .. })))
    );
    // Re-desiring before the old physical removal keeps settings and waits for that receipt.
    assert!(admitted(&f.sub(true, 3)).is_empty());
    assert_eq!(admitted(&f.closed(4)), vec![true]);
    f.sub(false, 5);
    f.closed(6);
    assert_eq!(
        admitted(&f.sub(true, 7)),
        vec![false],
        "real close+undesired cleanup drops settings immediately, not at unsubscribe+60s"
    );
}
#[test]
fn settings_after_unsubscribe_are_kept_but_subscribe_clears_the_keep_window() {
    let mut f = fixture();
    f.sub(true, 0);
    f.sub(false, 1);
    f.settings(2);
    f.closed(3);
    f.tick(60);
    assert_eq!(admitted(&f.sub(true, 60)), vec![true]);
    f.sub(false, 61);
    f.closed(62);
    assert_eq!(
        admitted(&f.sub(true, 63)),
        vec![false],
        "setDesired(true) clears the former keepUntil"
    );
}
#[test]
fn settings_only_unsubscribe_does_not_reset_keep_and_desired_updates_do_not_start_it() {
    let mut f = fixture();
    f.settings(0);
    f.sub(false, 20);
    f.tick(60);
    assert_eq!(admitted(&f.sub(true, 60)), vec![false]);
    // Desired entries keep settings indefinitely; settings updates do not start an undesired TTL.
    let mut f = fixture();
    f.settings(0);
    let effects = f.r.take_effects();
    assert!(effects.is_empty());
    f.r.handle_request(
        &f.b.sid,
        f.b.generation,
        SessionRequest::Subscription {
            track_sids: vec![f.track.clone()],
            subscribe: true,
        },
        f.at + Duration::from_secs(10),
    );
    let actual = f.r.take_effects();
    assert_eq!(admitted(&actual), vec![true]);
    f.tick(120);
    f.settings(121);
    f.sub(false, 122);
    f.closed(123);
    assert_eq!(
        admitted(&f.sub(true, 124)),
        vec![false],
        "settings updated while desired do not create undesired keepUntil"
    );
}
