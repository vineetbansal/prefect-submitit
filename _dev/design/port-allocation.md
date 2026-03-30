# Design: UID-Based Port Allocation for Multi-User Nodes

**Date:** 2026-03-27
**Status:** Draft
**Scope:** `src/prefect_submitit/server/config.py`

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

## Design Decision: Shared Modulo Offset

Extract the existing `os.getuid() % 800` pattern into a shared helper. All
service ports apply the same offset to their respective base port, keeping
them correlated and predictable.

```python
# Port ranges:
#   Prefect server: 4200-4999
#   PostgreSQL:     5433-6232

_PREFECT_BASE_PORT = 4200
_PG_BASE_PORT = 5433


def _user_port_offset() -> int:
    """Stable per-user offset for all service ports.

    Returns:
        Integer in [0, 800).
    """
    return os.getuid() % 800


def default_port() -> int:
    """Prefect server port, unique per user."""
    return _PREFECT_BASE_PORT + _user_port_offset()


def default_pg_port() -> int:
    """PostgreSQL port, unique per user."""
    return _PG_BASE_PORT + _user_port_offset()
```

### Why a single offset instead of independent port functions?

Correlated ports are easier to reason about: if you know a user's Prefect
port is `4200 + X`, their PG port is always `5433 + X`. This makes
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

## Migration

The change adds `default_pg_port()` and extracts `_user_port_offset()` as a
shared helper. `default_port()` behavior is unchanged (`4200 + uid % 800`).
Existing users whose server is running will not be affected until they
restart. On restart:

1. **Prefect port is unchanged.** The computation is the same; only the
   internal structure changes (shared offset helper).

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

The change is backwards-compatible — no data is lost, the PG data directory
is reused, and the Prefect port does not change. Only the PostgreSQL
listening port changes.

---

## Testing

- Unit test `_user_port_offset()` returns consistent values for the same UID.
- Unit test that Prefect and PG ports are correlated (same offset).
- Unit test that the offset is in `[0, 800)` for a range of UIDs.
- Update `test_config.py`: remove `DEFAULT_PG_PORT` import, update
  `test_defaults` to assert `config.pg_port == default_pg_port()`.
- Integration: start two servers under different UIDs on the same node
  (existing `test-slurm` infra can validate this if run by multiple users,
  but a direct multi-UID test requires `sudo` or containers).

---

## Scope of Change

| File | Change |
| ---- | ------ |
| `server/config.py` | Add `_user_port_offset()`, `default_pg_port()`. Refactor `default_port()` to use shared offset. Remove `DEFAULT_PG_PORT` constant. Update `make_config()` to call `default_pg_port()` and update its docstring (`pg_port` default changes from "5433" to "UID-based port"). |
| `server/__init__.py` | Update `start()` docstring: `pg_port` default description changes from "5433" to "UID-based port". |
| `server/postgres.py` | No change (already receives port via config). |
| `server/cli.py` | No change (already passes config through). |
| `tests/test_server/test_config.py` | Remove `DEFAULT_PG_PORT` import. Update `test_defaults` to assert against `default_pg_port()`. Add tests for `_user_port_offset()` and `default_pg_port()`. |
