# `tinio-auth` — the S3 authn/authz engine

`tinio-auth` is the engine behind tinio's enforced data plane, and it implements s3s's two seams: `ConfigAuth` is the `S3Auth` secret lookup a request's signature is verified against, and `AclAccess` is the `S3Access` pipeline that authorizes every operation before its route runs — principal resolution, the expected-owner header, the operation→requirement truth table, the owner bypass, grant evaluation, and the fail-closed posture on storage errors. Beside them the crate carries the canned-ACL expansion, the request-level `x-amz-grant-*` parsing and the one parse authority for the copy/rename source headers.

It owns neither the protocol layer nor the inputs to it: signature verification itself, the error-to-XML mapping, and the `acl` gate (the cargo feature and the `Capabilities` toggle behind it) are `tinio-server`'s, and the identity map arrives already assembled from `[auth]`/`[owner]`/`[[users]]` — that assembly, and every config-validation refusal behind it, is `tinio-server`'s `identity` module over `tinio-config`'s schema. The ACL domain types, their wire codecs and the group URIs are `tinio-core`'s, and so is the `Storage` the pipeline reads stored rows through. This crate has no features of its own.

## Signature and credentials

A configured user is a signing principal: an access key with its secret, a canonical ID, and a display name. The access key is the map's only index — `get_secret_key` answers that user's secret for s3s to verify the request against, and the canonical ID the same key resolves to is the principal every later comparison uses, against both ACL grantees and a resource owner's own ID. The inverse direction is carried too: an `Owner` element renders the configured display name, while an unrecognized ID — the anonymous principal included — renders ID-only, AWS's unknown-account shape.

Verification is s3s's, and so is which signature schemes it accepts: this crate configures nothing about them and never sees a request that failed one. What it contributes to that step is the secret, or its refusal — an access key the identity map does not carry is `InvalidAccessKeyId`, and the same refusal is raised again if principal resolution meets that key, so an unconfigured key reads identically wherever it is noticed instead of panicking.

A request with no credentials is not refused outright: it is evaluated as the anonymous principal, the special canonical ID `tinio-core` publishes, which owns nothing and holds nothing implicitly. It is authorized only by a grant that covers it — an AllUsers grant — and the root-level operations answer it the same fail-closed denial as any other ungranted principal, rather than a filtered result.

## Authorization

| Rule | What it is |
|---|---|
| The pipeline order | The `x-amz-expected-bucket-owner` header first — a mismatch, or a value that will not read as UTF-8, denies rather than skipping — then the DeleteObjects coarse gate, then the operation→requirement mapping, then the resource gate: owner bypass, and only for a non-owner, grants. |
| Requirement classes | Bucket and object data READ/WRITE, bucket READ_ACP/WRITE_ACP, object READ_ACP/WRITE_ACP, owner parity (which no grant can satisfy), any signed principal, and creator-or-bucket-owner. |
| Overlays | An operation may additionally require a source object's READ (`CopyObject`, `UploadPartCopy`, `RenameObject`), a destination bucket's WRITE, or dispatch on the destination key's existence — an existing key demands destination-owner parity, so a WRITE grantee creates keys but never overwrites another principal's object. |
| Group grantees | AllUsers matches any principal, the anonymous one included; AuthenticatedUsers matches any signed principal; LogDelivery and any unknown group URI are never satisfiable — fail closed. |
| Upload-scoped operations | The upload row the `uploadId` query parameter names, whose stored owner is the creator the creator-or-bucket-owner rule evaluates — the route path alone cannot identify one of several uploads in flight for a key. |
| `DeleteObjects` | The whole-operation gate is deliberately coarse — the bucket owner or holder of any bucket grant — because a WRITE-gated refusal would wrongly block a valid per-key delete; the per-key decisions belong to the handler. |
| An unmapped operation | Resolves to owner-only, never to anonymous: a resolved operation the table does not name is refused to a non-owner instead of being left open. A path s3s cannot resolve to an operation at all never reaches this pipeline. |
| Storage errors | A storage failure while the decision is being evaluated denies. `NoSuchBucket`/`NoSuchKey` is the deny-first three tiers: the owner, or a bucket-READ grantee, passes through to the handler's 404/204; a genuinely missing bucket passes through to its own `NoSuchBucket`; anything else is denied — a principal with no claim on the resource learns nothing from the refusal. |

## Refusals

| Refused | Answers |
|---|---|
| an access key the identity map does not carry | `InvalidAccessKeyId` |
| a principal that is neither the resource owner nor covered by a satisfying grant — the anonymous principal included | `AccessDenied`, the fail-closed default |
| `x-amz-expected-bucket-owner` naming another owner, or carrying a value that will not read as UTF-8 | `AccessDenied` |
| a copy or rename source that is absent, malformed, or an access-point/outpost ARN | `AccessDenied` |
| a storage error encountered while the decision is evaluated | `AccessDenied` |
| a canned ACL outside the eight documented names, a canned ACL combined with `x-amz-grant-*` headers, an unknown group URI, a malformed canonical ID, an email grantee, or a grant set past the cap | `InvalidArgument` |

The last row is the write-request input path, and it is strict on purpose; the codec that decodes a *stored* ACL row is a different path in `tinio-core` and self-heals instead. The canned names expand per the AWS table, with the two bucket-owner names meaningful only on an object write (ignored on a bucket ACL) and `aws-exec-read` accepted as private, no EC2 group constant existing to grant.

## Layout

| File | What it is |
|---|---|
| `src/identity.rs` | the configured-principal model: the access-key map, canonical IDs, the default owner element, the anonymous principal, principal and display-name resolution, with `tinio-core`'s canonical-ID derivation re-exported |
| `src/auth.rs` | `ConfigAuth`, the s3s `S3Auth` secret lookup |
| `src/access.rs` | `AclAccess`, the s3s `S3Access` pipeline: the resource gates, the owner bypass, grant evaluation, the existence head, the deny-first tiers |
| `src/matrix.rs` | the operation→requirement truth table and its overlays |
| `src/canned.rs` | the canned-ACL expansion and the request-level `x-amz-grant-*` parsing, behind one entry that yields the request's ACL |
| `src/source.rs` | the one parse authority for `x-amz-copy-source` and `x-amz-rename-source` |
| `src/error.rs` | the access-decision vocabulary (`Allow` / `Deny` / `PassThroughMissing`) and the storage-error classification |

## Gates

```console
cargo test -p tinio-auth
```

The crate is unit-tested on its own: the decision vocabulary, the truth table, the canned expansions and the grant parsing. The server that wires it is `tinio-server`'s `acl` feature (on by default), and the end-to-end leg that drives signed requests and real grants is `tinio-e2e`'s `acl.feature`.
