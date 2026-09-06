//! Canned-ACL expansion and request-level grant parsing (spec §5
//! write-path headers).
//!
//! The grant sets here are the request-input form: strict validation —
//! unknown group URI, malformed canonical ID, email grantees, the
//! 100-grant cap — answers 400 `InvalidArgument`. Request input is never
//! routed through the storage codec's self-heal (`Acl::from_grants_wire`),
//! which applies to stored-row decode only; the two paths stay distinct.

use s3s::{S3Error, s3_error};

use crate::_core::acl::{
    ACL_GRANTS_MAX, Acl, AclGrants, GROUP_ALL_USERS, GROUP_AUTHENTICATED_USERS, GROUP_LOG_DELIVERY,
    Grant, Grantee, GroupUri, OwnerId, Permission,
};

/// The pre-parsed `x-amz-grant-*` header values of one write request.
#[derive(Debug, Default, Clone, Copy)]
pub struct GrantHeaders<'a> {
    /// `x-amz-grant-full-control`
    pub full_control: Option<&'a str>,
    /// `x-amz-grant-read`
    pub read: Option<&'a str>,
    /// `x-amz-grant-read-acp`
    pub read_acp: Option<&'a str>,
    /// `x-amz-grant-write`
    pub write: Option<&'a str>,
    /// `x-amz-grant-write-acp`
    pub write_acp: Option<&'a str>,
}

impl GrantHeaders<'_> {
    /// Whether any grant header is present (canned ACLs and grant
    /// headers are mutually exclusive — a request carrying both is a
    /// 400 `InvalidArgument`).
    pub fn requested(&self) -> bool {
        self.full_control.is_some()
            || self.read.is_some()
            || self.read_acp.is_some()
            || self.write.is_some()
            || self.write_acp.is_some()
    }
}

/// The grants of a canned bucket ACL: the owner's `FULL_CONTROL` plus
/// the canned expansion. `bucket-owner-read`/`bucket-owner-full-control`
/// are ignored on buckets (private, per AWS); `aws-exec-read` is
/// accepted but private (the EC2 grant is dropped — no EC2 group
/// constant exists). An unknown canned value → 400 `InvalidArgument`.
pub fn canned_bucket_grants(owner: &OwnerId, canned: &str) -> Result<AclGrants, S3Error> {
    canned_grants(owner, None, canned)
}

/// The grants of a canned object ACL: the owner's `FULL_CONTROL` plus
/// the canned expansion; `bucket-owner-read`/`bucket-owner-full-control`
/// additionally grant the bucket owner. An unknown canned value → 400
/// `InvalidArgument`.
pub fn canned_object_grants(
    owner: &OwnerId,
    bucket_owner: &OwnerId,
    canned: &str,
) -> Result<AclGrants, S3Error> {
    canned_grants(owner, Some(bucket_owner), canned)
}

fn canned_grants(
    owner: &OwnerId,
    bucket_owner: Option<&OwnerId>,
    canned: &str,
) -> Result<AclGrants, S3Error> {
    let mut grants = vec![canonical_grant(owner, Permission::FullControl)];
    match canned {
        "private" | "aws-exec-read" => {
            // Owner FULL_CONTROL only — the EC2 grant has no constant.
        }
        "public-read" => {
            push_grant(&mut grants, group_grant(GROUP_ALL_USERS, Permission::Read));
        }
        "public-read-write" => {
            push_grant(&mut grants, group_grant(GROUP_ALL_USERS, Permission::Read));
            push_grant(&mut grants, group_grant(GROUP_ALL_USERS, Permission::Write));
        }
        "authenticated-read" => {
            push_grant(
                &mut grants,
                group_grant(GROUP_AUTHENTICATED_USERS, Permission::Read),
            );
        }
        "bucket-owner-read" => {
            if let Some(bo) = bucket_owner {
                push_grant(&mut grants, canonical_grant(bo, Permission::Read));
            }
        }
        "bucket-owner-full-control" => {
            if let Some(bo) = bucket_owner {
                push_grant(&mut grants, canonical_grant(bo, Permission::FullControl));
            }
        }
        "log-delivery-write" => {
            push_grant(
                &mut grants,
                group_grant(GROUP_LOG_DELIVERY, Permission::Write),
            );
            push_grant(
                &mut grants,
                group_grant(GROUP_LOG_DELIVERY, Permission::ReadAcp),
            );
        }
        _ => {
            return Err(s3_error!(InvalidArgument, "unknown canned ACL: {canned}"));
        }
    }
    Ok(grants)
}

/// Parse one request's `x-amz-grant-*` header set (`id="…"` /
/// `uri="…"` comma-separated lists per header). The owner's
/// `FULL_CONTROL` grant opens the set; strict validation rejects
/// unknown group URIs, malformed canonical IDs, email grantees and a
/// grant set over [`ACL_GRANTS_MAX`] — all 400 `InvalidArgument`.
pub fn grants_from_headers(owner: &OwnerId, hs: &GrantHeaders<'_>) -> Result<AclGrants, S3Error> {
    let mut grants = vec![canonical_grant(owner, Permission::FullControl)];
    let fields = [
        (hs.full_control, Permission::FullControl),
        (hs.read, Permission::Read),
        (hs.read_acp, Permission::ReadAcp),
        (hs.write, Permission::Write),
        (hs.write_acp, Permission::WriteAcp),
    ];
    for (value, permission) in fields {
        let Some(value) = value else { continue };
        for atom in value.split(',').map(str::trim).filter(|a| !a.is_empty()) {
            let grantee = grantee_from_atom(atom)?;
            push_grant(
                &mut grants,
                Grant {
                    grantee,
                    permission,
                },
            );
            if grants.len() > ACL_GRANTS_MAX {
                return Err(s3_error!(
                    InvalidArgument,
                    "ACL grant set exceeds the maximum of {ACL_GRANTS_MAX} grants"
                ));
            }
        }
    }
    Ok(grants)
}

/// Compose the full grants-only `Acl` of a write request: canned
/// expansion or grant headers — a canned ACL combined with any grant
/// header is a 400 `InvalidArgument`. The returned row carries
/// `owner: None`; the caller passes the owner element separately (the
/// storage row keeps owner and grants apart).
///
/// `bucket_owner` is `Some` on the object write surface (the
/// `bucket-owner-*` canned ACLs reference it) and `None` on the bucket
/// write surface (those names are ignored there — private, per AWS).
pub fn expand_acl(
    owner: &OwnerId,
    bucket_owner: Option<&OwnerId>,
    canned: Option<&str>,
    hs: GrantHeaders<'_>,
) -> Result<Acl, S3Error> {
    let grants = match canned {
        Some(canned) => {
            if hs.requested() {
                return Err(s3_error!(
                    InvalidArgument,
                    "cannot combine a canned ACL with x-amz-grant-* headers"
                ));
            }
            canned_grants(owner, bucket_owner, canned)?
        }
        None => grants_from_headers(owner, &hs)?,
    };
    Ok(Acl {
        owner: None,
        grants,
    })
}

fn grantee_from_atom(atom: &str) -> Result<Grantee, S3Error> {
    if let Some(id) = atom.strip_prefix("id=") {
        let id = unquote(id);
        OwnerId::new(id).map(Grantee::Canonical).map_err(|_| {
            s3_error!(
                InvalidArgument,
                "invalid canonical ID in grant header: {id}"
            )
        })
    } else if let Some(uri) = atom.strip_prefix("uri=") {
        let uri = GroupUri(unquote(uri).to_string());
        if uri.is_valid() {
            Ok(Grantee::Group(uri))
        } else {
            Err(s3_error!(
                InvalidArgument,
                "invalid group URI in grant header: {}",
                uri.0
            ))
        }
    } else {
        // An `email="…"` atom or a bare address — email grantees are
        // rejected (no email→ID resolution exists).
        Err(s3_error!(
            InvalidArgument,
            "email grantees are not supported: {atom}"
        ))
    }
}

/// Strip one pair of surrounding double quotes (the AWS header form).
fn unquote(s: &str) -> &str {
    let s = s.trim();
    s.strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(s)
}

fn canonical_grant(id: &OwnerId, permission: Permission) -> Grant {
    Grant {
        grantee: Grantee::Canonical(id.clone()),
        permission,
    }
}

fn group_grant(uri: &str, permission: Permission) -> Grant {
    Grant {
        grantee: Grantee::Group(GroupUri(uri.to_string())),
        permission,
    }
}

fn push_grant(grants: &mut AclGrants, grant: Grant) {
    if !grants.contains(&grant) {
        grants.push(grant);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::acl::{
        Acl, GROUP_ALL_USERS, GROUP_AUTHENTICATED_USERS, GROUP_LOG_DELIVERY, Grant, Grantee,
        GroupUri, OwnerId, Permission,
    };

    fn owner() -> OwnerId {
        OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").unwrap()
    }

    fn bucket_owner() -> OwnerId {
        OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100").unwrap()
    }

    fn canon(id: &OwnerId, permission: Permission) -> Grant {
        Grant {
            grantee: Grantee::Canonical(id.clone()),
            permission,
        }
    }

    fn group(uri: &str, permission: Permission) -> Grant {
        Grant {
            grantee: Grantee::Group(GroupUri(uri.to_string())),
            permission,
        }
    }

    fn asserting_invalid(err: &s3s::S3Error) {
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }

    #[test]
    fn canned_private_is_owner_full_control_only() {
        let o = owner();
        let private = Acl::default_private(Some(o.clone())).grants;
        assert_eq!(canned_bucket_grants(&o, "private").unwrap(), private);
        assert_eq!(
            canned_object_grants(&o, &bucket_owner(), "private").unwrap(),
            private
        );
    }

    #[test]
    fn canned_public_read_expands_all_users_read() {
        let o = owner();
        assert_eq!(
            canned_bucket_grants(&o, "public-read").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_ALL_USERS, Permission::Read),
            ]
        );
        assert_eq!(
            canned_object_grants(&o, &bucket_owner(), "public-read").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_ALL_USERS, Permission::Read),
            ]
        );
    }

    #[test]
    fn canned_public_read_write_expands_all_users_read_and_write() {
        let o = owner();
        assert_eq!(
            canned_bucket_grants(&o, "public-read-write").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_ALL_USERS, Permission::Read),
                group(GROUP_ALL_USERS, Permission::Write),
            ]
        );
    }

    #[test]
    fn canned_authenticated_read_expands_authenticated_users_read() {
        let o = owner();
        assert_eq!(
            canned_object_grants(&o, &bucket_owner(), "authenticated-read").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_AUTHENTICATED_USERS, Permission::Read),
            ]
        );
    }

    #[test]
    fn canned_aws_exec_read_is_accepted_but_private() {
        // The EC2 grant is dropped — no EC2 group constant exists (grilling Q5).
        let o = owner();
        let private = Acl::default_private(Some(o.clone())).grants;
        assert_eq!(
            canned_object_grants(&o, &bucket_owner(), "aws-exec-read").unwrap(),
            private
        );
    }

    #[test]
    fn canned_log_delivery_write_expands_log_delivery_write_and_read_acp() {
        let o = owner();
        assert_eq!(
            canned_bucket_grants(&o, "log-delivery-write").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_LOG_DELIVERY, Permission::Write),
                group(GROUP_LOG_DELIVERY, Permission::ReadAcp),
            ]
        );
    }

    #[test]
    fn canned_bucket_owner_read_is_object_only() {
        let (o, bo) = (owner(), bucket_owner());
        assert_eq!(
            canned_object_grants(&o, &bo, "bucket-owner-read").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                canon(&bo, Permission::Read)
            ]
        );
        // On a bucket the canned ACL is ignored — private, per AWS.
        let private = Acl::default_private(Some(o.clone())).grants;
        assert_eq!(
            canned_bucket_grants(&o, "bucket-owner-read").unwrap(),
            private
        );
    }

    #[test]
    fn canned_bucket_owner_full_control_is_object_only_and_dedupes_same_owner() {
        let (o, bo) = (owner(), bucket_owner());
        assert_eq!(
            canned_object_grants(&o, &bo, "bucket-owner-full-control").unwrap(),
            vec![
                canon(&o, Permission::FullControl),
                canon(&bo, Permission::FullControl)
            ]
        );
        // Same owner on both sides — the duplicate grant collapses.
        assert_eq!(
            canned_object_grants(&o, &o, "bucket-owner-full-control").unwrap(),
            vec![canon(&o, Permission::FullControl)]
        );
        // On a bucket the canned ACL is ignored — private, per AWS.
        let private = Acl::default_private(Some(o.clone())).grants;
        assert_eq!(
            canned_bucket_grants(&o, "bucket-owner-full-control").unwrap(),
            private
        );
    }

    #[test]
    fn canned_unknown_value_rejected() {
        let o = owner();
        asserting_invalid(&canned_bucket_grants(&o, "not-a-canned-acl").unwrap_err());
        asserting_invalid(
            &canned_object_grants(&o, &bucket_owner(), "not-a-canned-acl").unwrap_err(),
        );
    }

    #[test]
    fn grants_from_headers_parses_id_and_uri_lists() {
        let (o, u1, u2) = (owner(), bucket_owner(), owner());
        let hs = GrantHeaders {
            full_control: Some(
                r#"id="ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100""#,
            ),
            read: Some(
                r#"id="aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899", uri="http://acs.amazonaws.com/groups/global/AllUsers""#,
            ),
            ..GrantHeaders::default()
        };
        let grants = grants_from_headers(&o, &hs).unwrap();
        assert_eq!(grants.len(), 4);
        assert!(grants.contains(&canon(&o, Permission::FullControl)));
        assert!(grants.contains(&canon(&u1, Permission::FullControl)));
        assert!(grants.contains(&canon(&u2, Permission::Read)));
        assert!(grants.contains(&group(GROUP_ALL_USERS, Permission::Read)));
    }

    #[test]
    fn grants_from_headers_rejects_email_grantee() {
        let o = owner();
        let hs = GrantHeaders {
            read: Some(r#"email="user@example.com""#),
            ..GrantHeaders::default()
        };
        asserting_invalid(&grants_from_headers(&o, &hs).unwrap_err());
        let hs = GrantHeaders {
            read: Some("user@example.com"),
            ..GrantHeaders::default()
        };
        asserting_invalid(&grants_from_headers(&o, &hs).unwrap_err());
    }

    #[test]
    fn request_grant_unknown_group_uri_rejected() {
        let o = owner();
        let hs = GrantHeaders {
            read: Some(r#"uri="http://evildomain/""#),
            ..GrantHeaders::default()
        };
        asserting_invalid(&grants_from_headers(&o, &hs).unwrap_err());
    }

    #[test]
    fn request_grant_malformed_canonical_id_rejected() {
        let o = owner();
        let hs = GrantHeaders {
            read: Some(r#"id="short""#),
            ..GrantHeaders::default()
        };
        asserting_invalid(&grants_from_headers(&o, &hs).unwrap_err());
        let hs = GrantHeaders {
            read: Some(r#"id="AABBCCDDEFF00112233445566778899AABBCCDDEFF00112233445566778899""#),
            ..GrantHeaders::default()
        };
        asserting_invalid(&grants_from_headers(&o, &hs).unwrap_err());
    }

    #[test]
    fn request_grant_over_cap_rejected() {
        let o = owner();
        // 101 declared grants — the owner full-control grant rides on top, still 400.
        let atoms: Vec<String> = (0..101).map(|i| format!(r#"id="{i:064x}""#)).collect();
        let value = atoms.join(",");
        let hs = GrantHeaders {
            full_control: Some(value.as_str()),
            ..GrantHeaders::default()
        };
        asserting_invalid(&grants_from_headers(&o, &hs).unwrap_err());
        // The cap boundary: 99 declared grants + the owner grant = exactly 100 → Ok.
        let atoms: Vec<String> = (0..99).map(|i| format!(r#"id="{i:064x}""#)).collect();
        let value = atoms.join(",");
        let hs = GrantHeaders {
            full_control: Some(value.as_str()),
            ..GrantHeaders::default()
        };
        assert_eq!(grants_from_headers(&o, &hs).unwrap().len(), 100);
    }

    #[test]
    fn expand_acl_carries_grants_only_and_keeps_the_owner_on_the_caller() {
        let (o, bo) = (owner(), bucket_owner());
        let acl = expand_acl(&o, Some(&bo), Some("public-read"), GrantHeaders::default()).unwrap();
        assert_eq!(acl.owner, None); // the caller passes the owner element separately
        assert_eq!(
            acl.grants,
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_ALL_USERS, Permission::Read),
            ]
        );
        let acl = expand_acl(&o, Some(&bo), None, GrantHeaders::default()).unwrap();
        assert_eq!(acl.owner, None);
        assert_eq!(acl.grants, Acl::default_private(Some(o.clone())).grants);
        let hs = GrantHeaders {
            read: Some(r#"uri="http://acs.amazonaws.com/groups/global/AllUsers""#),
            ..GrantHeaders::default()
        };
        let acl = expand_acl(&o, Some(&bo), None, hs).unwrap();
        assert_eq!(
            acl.grants,
            vec![
                canon(&o, Permission::FullControl),
                group(GROUP_ALL_USERS, Permission::Read),
            ]
        );
        // Bucket-owner-* on the object surface references the bucket owner.
        let acl = expand_acl(&o, Some(&bo), Some("bucket-owner-read"), GrantHeaders::default()).unwrap();
        assert_eq!(
            acl.grants,
            vec![
                canon(&o, Permission::FullControl),
                canon(&bo, Permission::Read)
            ]
        );
    }

    #[test]
    fn expand_acl_rejects_canned_plus_grant_headers() {
        let (o, bo) = (owner(), bucket_owner());
        let hs = GrantHeaders {
            read: Some(r#"uri="http://acs.amazonaws.com/groups/global/AllUsers""#),
            ..GrantHeaders::default()
        };
        asserting_invalid(&expand_acl(&o, Some(&bo), Some("public-read"), hs).unwrap_err());
    }
}
