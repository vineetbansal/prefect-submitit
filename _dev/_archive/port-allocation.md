# Design: UID-Based Port Allocation for Multi-User Nodes

**Date:** 2026-03-27
**Status:** Draft
**Scope:** `src/prefect_submitit/server/config.py`, `src/prefect_submitit/server/prefect_proc.py`

---

## Context

`prefect-submitit` manages a local Prefect server and its PostgreSQL backend
on shared HPC login/compute nodes. Multiple users on the same node may each
start their own server stack, so every service port must be unique per user.

Today the Prefect server port is UID-scoped:

```python
def default_port() -> int:
    return 4200 + (os.getuid() % 800)
```

But the PostgreSQL port is a global constant:

```python
DEFAULT_PG_PORT = 5433
```

If two users start servers on the same node, the second user's PostgreSQL
fails to bind. The data directory (`~/.prefect-submitit/postgres`) is already
per-user (under `$HOME`), so only the port is the problem.

---

## Design Constraints

**Must be deterministic.** A user's ports must be the same every time they
start a server, across sessions and nodes. No state file or registry.

**Must minimize collision risk.** HPC systems typically assign UIDs
sequentially (1000, 1001, 1002...). With a modulo range of 800 slots,
collisions are unlikely for typical HPC user counts. The `--pg-port` and
`--port` CLI flags provide an escape hatch when they do occur.

**Must be extensible.** If we add more services later (e.g., a metrics
collector), adding a port should be trivial.

**Must avoid well-known ports.** Ports must stay above 1023. The current
base ports (4200 for Prefect, 5433 for PostgreSQL) sit in the IANA
registered range, which is acceptable for a locally managed service stack.

---

## Design Decision: Shared Modulo Offset with Parity Stride

Extract the existing `os.getuid() % 800` pattern into a shared helper. All
service ports apply the same offset — multiplied by a stride of 2 — to their
respective base port. Because 4200 is even and 5433 is odd, the stride
ensures Prefect ports are always even and PostgreSQL ports are always odd.
Inter-service port collision is impossible by construction.

```python
# Port ranges (stride of 2 ensures parity separation):
#   Prefect server: 4200-5798  (even only)
#   PostgreSQL:     5433-7031  (odd only)

_PREFECT_BASE_PORT = 4200  # even
_PG_BASE_PORT = 5433       # odd


def _user_port_offset() -> int:
    """Stable per-user offset for all service ports.

    Returns:
        Integer in [0, 800).
    """
    return os.getuid() % 800


def default_port() -> int:
    """Prefect server port, unique per user."""
    return _PREFECT_BASE_PORT + 2 * _user_port_offset()


def default_pg_port() -> int:
    """PostgreSQL port, unique per user."""
    return _PG_BASE_PORT + 2 * _user_port_offset()
```

### Why a stride of 2?

The numerical ranges overlap (5433–5798 is shared territory), but since
Prefect ports are always even and PostgreSQL ports are always odd, no actual
port number can belong to both services. This eliminates inter-service
collision as a structural invariant rather than relying on careful base-port
placement.

### Why a single offset instead of independent port functions?

Correlated ports are easier to reason about: if you know a user's Prefect
port is `4200 + 2X`, their PG port is always `5433 + 2X`. This makes
debugging, log correlation, and firewall rules straightforward.

---

## Alternatives Considered

### 1. Port file / registry

A shared file (e.g., `/tmp/prefect-ports.json`) where each user claims a
port. Eliminates collisions entirely.

**Rejected:** Introduces shared mutable state on a filesystem. Stale entries
from crashed processes require cleanup logic. Lock contention on NFS. Far
more complexity than the problem warrants given the low collision probability.

### 2. Bind-and-retry

Try the default port; if it fails, increment and retry.

**Rejected:** Non-deterministic — a user's ports change depending on who
started first. Breaks the discovery file (other processes can't predict the
port). Would require the discovery file to become the source of truth, adding
a hard dependency on it being correct and up-to-date.

### 3. Let the OS assign (port 0)

Bind to port 0 and let the kernel assign an ephemeral port.

**Rejected:** Same problems as bind-and-retry — non-deterministic, breaks
cross-node discovery, and PostgreSQL's `pg_ctl` doesn't support port 0
cleanly.

### 4. Wider modulo range

`uid % 16000` to reduce collision probability, using high ports (49152-65535).

**Rejected:** High ports conflict with the OS ephemeral range
(`/proc/sys/net/ipv4/ip_local_port_range`, typically 32768-60999). Also
scatters ports across a wide range, making firewall rules harder.

### 5. Hash-based offset (SHA-256)

`sha256(uid) % 800` instead of `uid % 800` for uniform distribution.

**Rejected:** Collision probability is identical (same 800 slots). Adds a
`hashlib` dependency and makes the offset non-obvious to compute manually.
Plain modulo is simpler and already the established pattern for
`default_port()`.

---

## Error Handling: Port Conflicts

### Current State

PostgreSQL startup has robust port-conflict handling via
`_kill_orphan_on_port()` in `postgres.py`:

- Identifies the process holding the port (via `lsof`, with socket fallback).
- Kills orphan PostgreSQL processes automatically.
- Raises a clear `RuntimeError` if a non-PostgreSQL process holds the port.

Prefect server startup has **no** port-conflict handling. If the port is
already in use, `prefect server start` fails with a raw subprocess error.
In background mode, `_wait_for_healthy_or_death` catches the early exit and
shows log tail, but the message doesn't identify the cause or suggest a fix.

### Design Decision: Post-Failure Detection, Not Pre-Checks

Port availability pre-checks suffer from a TOCTOU (time-of-check,
time-of-use) race: the port can become occupied between the check and the
actual bind. Instead:

1. **Attempt to start.** Let the service's own bind fail naturally.
2. **Detect the failure mode.** Scan the log output for "address already in
   use" (or equivalent).
3. **Provide actionable guidance.** A targeted error message with recovery
   steps.

This mirrors the PostgreSQL approach: the value is in the error message, not
in a racy pre-check.

### Prefect Server: Proposed Error Message

When `_wait_for_healthy_or_death` detects an early exit and the log contains
an address-in-use indicator, replace the generic startup-failure message with:

```
Port {port} is already in use.

If this is a stale prefect-submitit server:
    prefect-server stop
    prefect-server start --restart

To use a different port:
    prefect-server start --port <port>
```

For foreground mode (no log file), the same detection applies to stderr.

### Why not detect-and-kill for Prefect?

The PostgreSQL orphan-kill logic is justified because `pg_ctl` commonly
leaves orphan processes when pid files are deleted. Prefect server processes
are less prone to orphaning, and killing an arbitrary process on the port
would be dangerous. For Prefect, **detect and inform** is the right level of
intervention.

---

## Migration

The change adds `default_pg_port()` and extracts `_user_port_offset()` as a
shared helper. `default_port()` behavior is unchanged (`4200 + uid % 800`).
Existing users whose server is running will not be affected until they
restart. On restart:

1. **Prefect port changes from `4200 + uid % 800` to `4200 + 2 * (uid % 800)`.**
   For users with offset 0 the port is unchanged (4200). For others, the port
   shifts by at most +799 (e.g., offset 1: 4201 → 4202). The discovery file
   is rewritten on every start, so downstream consumers pick up the new port
   automatically.

2. **PostgreSQL port changes from 5433 to a UID-based port.** The existing
   PG data directory (`~/.prefect-submitit/postgres`) stores the port in
   `postgresql.conf`, which is rewritten on every start via
   `_write_custom_config()`. No manual migration needed.

3. **Environment variables** — users who hardcoded PostgreSQL port 5433 in
   scripts will need to update. The Prefect port is unchanged. Document in
   release notes.

4. **Collision escape hatch** — if two users' UIDs collide
   (`uid_a % 800 == uid_b % 800`), the `--pg-port` and `--port` CLI flags
   allow manual override.

The change is backwards-compatible — no data is lost and the PG data
directory is reused. Both ports shift slightly (Prefect by up to +799,
PostgreSQL from a fixed constant to a UID-based value), but the discovery
file and `postgresql.conf` are rewritten on every start, so no manual
intervention is needed.

---

## Testing

**Port allocation:**

- Unit test `_user_port_offset()` returns consistent values for the same UID.
- Unit test that Prefect and PG ports are correlated (same offset).
- Unit test that the offset is in `[0, 800)` for a range of UIDs.
- Unit test **parity invariant**: `default_port()` is always even,
  `default_pg_port()` is always odd, for a range of UIDs.
- Unit test that the two port ranges never produce the same port number.
- Update `test_config.py`: remove `DEFAULT_PG_PORT` import, update
  `test_defaults` to assert `config.pg_port == default_pg_port()`.
- Integration: start two servers under different UIDs on the same node
  (existing `test-slurm` infra can validate this if run by multiple users,
  but a direct multi-UID test requires `sudo` or containers).

**Error handling:**

- Unit test that `_wait_for_healthy_or_death` produces the port-conflict
  error message when the log contains "address already in use".
- Unit test that non-port-related failures still produce the generic
  startup-failure message (no regression).

---

## Scope of Change

| File | Change |
| ---- | ------ |
| `server/config.py` | Add `_user_port_offset()`, `default_pg_port()`. Refactor `default_port()` to use `2 * offset` stride. Remove `DEFAULT_PG_PORT` constant. Update `make_config()` to call `default_pg_port()` and update its docstring. |
| `server/__init__.py` | Update `start()` docstring: `pg_port` default description changes from "5433" to "UID-based port". |
| `server/prefect_proc.py` | In `_wait_for_healthy_or_death`, detect "address already in use" in log output and raise a targeted error message with recovery steps. Add equivalent detection for foreground mode stderr. |
| `server/postgres.py` | No change (already receives port via config). |
| `server/cli.py` | No change (already passes config through). |
| `tests/test_server/test_config.py` | Remove `DEFAULT_PG_PORT` import. Update `test_defaults` to assert against `default_pg_port()`. Add tests for `_user_port_offset()`, `default_pg_port()`, and parity invariant. |
| `tests/test_server/test_prefect_proc.py` | Add test that address-in-use log output produces the targeted error message. |
