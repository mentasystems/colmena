# Upgrading Colmena

Most Colmena upgrades are ordinary dependency bumps: update the version, deploy
your nodes however you normally do (including a one-at-a-time rolling deploy),
done. The decode side reads every format version Colmena has ever written, so a
newer node always understands an older cluster's existing Raft log and snapshots.

The one upgrade that needs care is a **wire-format bump** — a release that
increases `CommandFormatVersion` or `SnapshotFormatVersion` (see `version.go`).
Crossing one of those is a **format migration**, not a plain deploy.

## Why a format bump is special

A node writes Raft log entries in the newest format it knows. If an upgraded
node becomes (or reaches) the leader and replicates new-format entries while
some nodes are still on the old release, those older nodes **cannot decode the
entries** and log, in a tight loop:

```
colmena: fsm apply ... unsupported format version
```

While that happens the old nodes silently fail to apply committed entries — i.e.
their state diverges from the quorum. This is exactly what bit a downstream
3-node cluster: a normal one-at-a-time rolling deploy across the v1→v2
`CommandFormatVersion` bump wedged the not-yet-upgraded nodes, and recovery
required rolling the odd node back and redeploying every node together.

## How Colmena protects you now (write-version negotiation)

As of the version-negotiation change, the leader **does not write a newer format
until every current voter has advertised it can read it.** Each node advertises
its supported `CommandFormatVersion` / `SnapshotFormatVersion` in the RPC `Hello`
handshake; the leader continuously probes voters and writes at the
`effectiveWriteVersion = min(supported version across all voters)`. It bumps to
the new format only once **all voters** report support, and re-marshals forwarded
writes at that effective version so the rule holds no matter which node
originated a write.

The practical effect: **a one-at-a-time rolling deploy across a format bump is
now safe by construction.** While the cluster is mixed-version, the leader keeps
writing the old format that every node can read; when the last node finishes
upgrading, the leader flips to the new format on its own.

You can watch the migration:

- `Node.FormatStatus()` reports `Skew`, `EffectiveCommandVersion`, and the
  `PeersBehind` / `PeersAhead` voter lists.
- The metrics endpoint exposes `colmena_format_skew` (1 while mid-migration),
  `colmena_command_format_effective`, `colmena_command_format_local`, and
  `colmena_format_rejects_total`.
- A non-zero, growing `colmena_format_rejects_total` (logged at ERROR as
  "fsm apply REFUSED log entry … state is diverging") means a node is receiving
  entries it cannot decode — wire a health check / readiness probe to fail on it.

### Recommended rollout for a format bump

1. Deploy the new version to your nodes one at a time, as usual.
2. (Optional) Watch `FormatStatus().Skew` / `colmena_format_skew` go `true` while
   the deploy is in progress and back to `false` once every node is upgraded.
3. Nothing else to do — the leader flips to the new format automatically once all
   voters support it.

## Manual fallback (clusters without write-version negotiation)

If you are upgrading **from** a release that predates write-version negotiation
(its nodes don't hold back), the protection only fully applies once every node
is running a version that has it. Until then, treat a format bump as a manual
migration and avoid a mixed-format window:

- **Deploy all nodes together** — Fly: `fly deploy --strategy immediate`; bare
  metal / containers: stop all nodes, then start all nodes on the new version.
- **Do NOT do a one-at-a-time rolling deploy across the format bump.**

The new version can *read* both old and new formats (the decode switch in
`command.go` handles `case 1` / `case 2`), so the existing on-disk log and
snapshots are fine. The danger is purely **old nodes reading new writes** during
the transition — which an all-at-once deploy eliminates.

## Quick reference

| Situation | Safe procedure |
|---|---|
| Normal version bump (no format change) | Rolling deploy, any order |
| Format bump, all nodes already have negotiation | Rolling deploy — leader holds the old format until all upgrade, then flips |
| Format bump, upgrading away from a pre-negotiation release | Deploy all nodes together (`--strategy immediate` / stop-all-start-all) |

To check whether a release changes the wire format, compare `CommandFormatVersion`
and `SnapshotFormatVersion` in `version.go` between the two versions.
