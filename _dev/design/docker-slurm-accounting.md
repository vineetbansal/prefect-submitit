# Design: SLURM Accounting Storage for Docker Environment

**Date:** 2026-04-01
**Status:** Implemented
**Scope:** `docker/`

---

## Context

The Docker SLURM image (`nathanhess/slurm:full-v1.2.0`) ships with
`AccountingStorageType=accounting_storage/none`. This means `sacct` returns
no data, and submitit reports all job states as `UNKNOWN`.

A realistic dev environment should have full accounting so that:

- `sacct -j <jobid>` returns job history (useful for debugging)
- Submitit's native state detection works without fallbacks
- The dev environment matches production cluster behavior
- QoS, fairshare, and resource tracking can be tested

---

## Goals

1. `sacct` returns accurate job history inside the Docker container.
2. Minimal additions to image size and startup time.
3. No changes to the host workflow (`pixi run slurm-up` still works).

## Non-Goals

- Full multi-cluster accounting federation.
- Persistent job history across `slurm-down` / `slurm-up` cycles.
- User/account management (single user is sufficient).

---

## Proposed Solution

Add MariaDB and slurmdbd to the Docker image. SLURM's accounting requires
a three-component stack:

```
slurmctld <-> slurmdbd <-> MariaDB
```

### Dockerfile additions

```dockerfile
# MariaDB for SLURM accounting storage
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        mariadb-server slurmdbd && \
    rm -rf /var/lib/apt/lists/*

# slurmdbd configuration (contains DB password — restrict permissions)
COPY slurmdbd.conf /etc/slurm/slurmdbd.conf
RUN chown slurm:slurm /etc/slurm/slurmdbd.conf && \
    chmod 600 /etc/slurm/slurmdbd.conf
```

This adds ~50 MB to the image. The base image does not include `slurmdbd`.

### slurm.conf changes

The base image's `/etc/startup.sh` uses `sed` to template hostname, CPU,
and memory into `/etc/slurm/slurm.conf` at runtime. We add one more `sed`
in our entrypoint to enable accounting — this avoids maintaining a full
copy of the base config:

```bash
sed -i 's|AccountingStorageType=accounting_storage/none|AccountingStorageType=accounting_storage/slurmdbd|' /etc/slurm/slurm.conf
sed -i '/AccountingStorageType/a AccountingStorageHost=localhost' /etc/slurm/slurm.conf
```

### slurmdbd.conf

```
DbdHost=localhost
StorageType=accounting_storage/mysql
StorageHost=localhost
StorageLoc=slurm_acct_db
StorageUser=slurm
StoragePass=slurm
LogFile=/var/log/slurmdbd.log
PidFile=/run/slurmdbd.pid
```

### Entrypoint changes

The base image's `/etc/startup.sh` starts munge, slurmd, and slurmctld.
With accounting enabled, slurmctld requires slurmdbd to be running first.
We replace the single `/etc/startup.sh` call with explicit daemon startup
in the correct order:

```bash
#!/bin/bash
set -e

# --- Configure SLURM (replicate /etc/startup.sh sed commands) ---
sed -i "s/<<HOSTNAME>>/$(hostname)/" /etc/slurm/slurm.conf
sed -i "s/<<CPU>>/$(nproc)/" /etc/slurm/slurm.conf
REAL_MEM=$(slurmd -C | grep -oP 'RealMemory=\K[0-9]+')
sed -i "s/<<MEMORY>>/$REAL_MEM/" /etc/slurm/slurm.conf

# Enable accounting storage (replaces accounting_storage/none)
sed -i 's|AccountingStorageType=accounting_storage/none|AccountingStorageType=accounting_storage/slurmdbd|' /etc/slurm/slurm.conf
sed -i '/AccountingStorageType/a AccountingStorageHost=localhost' /etc/slurm/slurm.conf

# --- Start daemons in dependency order ---

# 1. Munge (auth for all SLURM daemons including slurmdbd)
service munge start

# 2. MariaDB (socket-only, no TCP)
mysqld_safe --skip-networking &
for i in $(seq 1 30); do
    mysqladmin ping --silent 2>/dev/null && break
    [ "$i" -eq 30 ] && { echo "ERROR: MariaDB did not start." >&2; exit 1; }
    sleep 1
done

# 3. Initialize accounting database (idempotent)
mysql -e "CREATE DATABASE IF NOT EXISTS slurm_acct_db;"
mysql -e "CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'slurm';"
mysql -e "GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost';"

# 4. slurmdbd
slurmdbd
for i in $(seq 1 30); do
    sacctmgr show cluster --noheader --parsable2 2>/dev/null && break
    [ "$i" -eq 30 ] && { echo "ERROR: slurmdbd did not start." >&2; exit 1; }
    sleep 1
done

# 5. Register cluster and default account (idempotent — errors ignored)
sacctmgr -i add cluster cluster 2>/dev/null || true
sacctmgr -i add account default cluster=cluster 2>/dev/null || true
sacctmgr -i add user docker account=default 2>/dev/null || true

# 6. SLURM compute and controller daemons
service slurmd start
service slurmctld start

# Wait for SLURM controller to accept commands
for i in $(seq 1 30); do
    if squeue --noheader 2>/dev/null; then
        echo "SLURM is ready."
        break
    fi
    [ "$i" -eq 30 ] && { echo "ERROR: SLURM did not become ready in 30s." >&2; exit 1; }
    sleep 1
done

# Fix ownership of the .pixi volume (Docker creates named volumes as root)
chown docker:docker /workspace/.pixi 2>/dev/null || true

# Drop to non-root user for the main process
exec runuser -u docker -- "$@"
```

### Startup order

```
munge -> MariaDB -> slurmdbd -> slurmctld + slurmd
  |         |          |             |
  auth    storage    accounting    scheduler
```

Each daemon depends on the one before it. The base image's `/etc/startup.sh`
is no longer called — its `sed` templating and `service` commands are
replicated directly in our entrypoint.

### Startup time impact

| Component | Estimated time |
|-----------|---------------|
| MariaDB start | ~2s |
| Database init (first run) | ~1s |
| slurmdbd start | ~1s |
| sacctmgr setup | ~1s |
| **Total added** | **~5s** |

Under QEMU emulation (Apple Silicon), multiply by ~3-5x: ~15-25s added.

---

## Alternatives Considered

**SQLite for slurmdbd:** slurmdbd requires MySQL/MariaDB/PostgreSQL.
SQLite is not supported.

**PostgreSQL (already in pixi):** The pixi-managed PostgreSQL runs as the
`docker` user in the container. slurmdbd typically runs as root or the
`slurm` user, and configuring it to share the pixi PostgreSQL would be
fragile. A dedicated MariaDB instance via apt is simpler and isolated.

**Accounting via squeue only (no slurmdbd):** squeue provides current job
state but no historical data. Good enough for state detection but not for
`sacct` queries or realistic cluster behavior.

**Keep using `/etc/startup.sh`:** The base image's startup script bundles
munge + slurmd + slurmctld in a single call. With accounting enabled,
slurmdbd must start before slurmctld. Decomposing the startup into our
entrypoint gives us full control over ordering.

---

## Files Changed

| File | Change |
|------|--------|
| `docker/Dockerfile` | Add `mariadb-server`, `slurmdbd` packages; copy slurmdbd.conf with restricted permissions |
| `docker/entrypoint.sh` | Replace `/etc/startup.sh` call with explicit daemon startup in dependency order |
| `docker/slurmdbd.conf` | New: slurmdbd configuration |

Note: `slurm.conf` is modified at runtime via `sed` in the entrypoint —
it is not a file in this repo (it lives inside the base image).

---

## Verification

1. `pixi run slurm-build` succeeds.
2. `pixi run slurm-up && pixi run slurm-shell` — container starts,
   SLURM is ready.
3. Inside container: submit a job, then `sacct -j <jobid>` returns state
   and resource usage.
4. Submitit reports accurate states (PENDING -> RUNNING -> COMPLETED),
   not UNKNOWN.
5. Cancel integration tests pass without squeue fallback.
6. `pixi run slurm-down` cleanly tears down everything.
7. Second `pixi run slurm-up` works (entrypoint is idempotent).

---

## Base Image Reference

Values verified against `nathanhess/slurm:full-v1.2.0`:

| Property | Value |
|----------|-------|
| `ClusterName` | `cluster` |
| `AccountingStorageType` | `accounting_storage/none` (we change this) |
| `slurmdbd` pre-installed | No |
| `/etc/startup.sh` order | `sed` templating -> munge -> slurmd -> slurmctld |
| Config path | `/etc/slurm/slurm.conf` |
