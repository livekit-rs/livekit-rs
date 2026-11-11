//! Permission predicates (`pkg/service/auth.go:172-259`). All fail with
//! "permissions denied" when no token was presented.

use super::VerifiedToken;

/// `ErrPermissionDenied`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("permissions denied")]
pub struct PermissionDenied;

type Auth<'a> = Option<&'a VerifiedToken>;

fn check(ok: bool) -> Result<(), PermissionDenied> {
    if ok { Ok(()) } else { Err(PermissionDenied) }
}

/// `EnsureJoinPermission`: returns the token's room.
pub fn ensure_join(auth: Auth<'_>) -> Result<&str, PermissionDenied> {
    let t = auth.ok_or(PermissionDenied)?;
    check(t.grants.video.room_join)?;
    Ok(t.grants.video.room.as_str())
}

/// `EnsureAdminPermission(room)`: `roomAdmin` and an exact room match.
pub fn ensure_admin(auth: Auth<'_>, room: &str) -> Result<(), PermissionDenied> {
    let t = auth.ok_or(PermissionDenied)?;
    check(t.grants.video.room_admin && t.grants.video.room == room)
}

pub fn ensure_create(auth: Auth<'_>) -> Result<(), PermissionDenied> {
    check(auth.is_some_and(|t| t.grants.video.room_create))
}

pub fn ensure_list(auth: Auth<'_>) -> Result<(), PermissionDenied> {
    check(auth.is_some_and(|t| t.grants.video.room_list))
}

pub fn ensure_record(auth: Auth<'_>) -> Result<(), PermissionDenied> {
    check(auth.is_some_and(|t| t.grants.video.room_record))
}

pub fn ensure_ingress_admin(auth: Auth<'_>) -> Result<(), PermissionDenied> {
    check(auth.is_some_and(|t| t.grants.video.ingress_admin))
}

pub fn ensure_sip_admin(auth: Auth<'_>) -> Result<(), PermissionDenied> {
    check(auth.is_some_and(|t| t.extra.sip.admin))
}

pub fn ensure_sip_call(auth: Auth<'_>) -> Result<(), PermissionDenied> {
    check(auth.is_some_and(|t| t.extra.sip.call))
}

/// `EnsureDestRoomPermission(src, dst)`.
pub fn ensure_dest_room(auth: Auth<'_>, src: &str, dst: &str) -> Result<(), PermissionDenied> {
    let t = auth.ok_or(PermissionDenied)?;
    let v = &t.grants.video;
    check(v.room_admin && v.room == src && v.destination_room == dst)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(f: impl FnOnce(&mut VerifiedToken)) -> VerifiedToken {
        let mut t = VerifiedToken::default();
        f(&mut t);
        t
    }

    #[test]
    fn admin_requires_exact_room() {
        let t = token(|t| {
            t.grants.video.room_admin = true;
            t.grants.video.room = "a".into();
        });
        assert!(ensure_admin(Some(&t), "a").is_ok());
        assert!(ensure_admin(Some(&t), "b").is_err());
        assert!(ensure_admin(None, "a").is_err());
        let wildcard = token(|t| t.grants.video.room_admin = true);
        assert!(ensure_admin(Some(&wildcard), "a").is_err());
    }

    #[test]
    fn dest_room() {
        let t = token(|t| {
            t.grants.video.room_admin = true;
            t.grants.video.room = "a".into();
            t.grants.video.destination_room = "b".into();
        });
        assert!(ensure_dest_room(Some(&t), "a", "b").is_ok());
        assert!(ensure_dest_room(Some(&t), "a", "c").is_err());
        assert_eq!(PermissionDenied.to_string(), "permissions denied");
    }
}
