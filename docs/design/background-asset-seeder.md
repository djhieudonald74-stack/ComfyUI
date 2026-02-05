# Background Asset Seeder Design Document

## Problem Statement

The `seed_assets` function in `app/assets/scanner.py` scans filesystem directories and imports assets into the database. Currently it runs synchronously, which causes two problems:

1. **Startup blocking**: When ComfyUI starts, `setup_database()` calls `seed_assets(["models"])` synchronously. If the models directory contains thousands of files, startup is delayed significantly before the UI becomes available.

2. **API request blocking**: The `POST /api/assets/seed` endpoint runs synchronously, blocking the HTTP request until scanning completes. For large directories, this causes request timeouts and a poor user experience.

## Goals

- Move asset scanning to a background thread so startup and API requests return immediately
- Provide visibility into scan progress via API and WebSocket events
- Support graceful cancellation of in-progress scans
- Maintain backward compatibility for tests that rely on synchronous behavior
- Ensure thread safety when accessing shared state

## Non-Goals

- Targeted/priority scanning of specific paths (addressed separately via synchronous `scan_paths()`)
- Parallel scanning across multiple threads
- Persistent scan state across restarts

---

## Architecture Overview

### Component: AssetSeeder Singleton

A new `AssetSeeder` class in `app/assets/seeder.py` manages background scanning with the following responsibilities:

- Owns a single background `threading.Thread` for scanning work
- Tracks scan state and progress in a thread-safe manner
- Provides cancellation support via `threading.Event`
- Emits WebSocket events for UI progress updates

### State Machine

```
IDLE ──start()──► RUNNING ──(completes)──► IDLE
                     │
                  cancel()
                     │y
                     ▼
                CANCELLING ──(thread exits)──► IDLE
```

### Integration Points

| Component | Change |
|-----------|--------|
| `main.py` | Call `asset_seeder.start()` (non-blocking) instead of `seed_assets()` |
| `main.py` | Call `asset_seeder.shutdown()` in `finally` block alongside `cleanup_temp()` |
| API routes | New endpoints for status and cancellation |
| WebSocket | Emit progress events during scanning |
| Test helper | Use `?wait=true` query param for synchronous behavior |

---

## Tasks

### Task 1: Create AssetSeeder Class

**Description**: Implement the core `AssetSeeder` singleton class with thread management, state tracking, and cancellation support.

**Acceptance Criteria**:
- [ ] `AssetSeeder` is a singleton accessible via module-level instance
- [ ] `State` enum with values: `IDLE`, `RUNNING`, `CANCELLING`
- [ ] `start(roots)` method spawns a daemon thread and returns immediately
- [ ] `start()` is idempotent—calling while already running is a no-op and returns `False`
- [ ] `cancel()` method signals the thread to stop gracefully
- [ ] `wait(timeout)` method blocks until thread completes or timeout expires
- [ ] `get_status()` returns current state and progress information
- [ ] All state access is protected by `threading.Lock`
- [ ] Thread creates its own database sessions (no session sharing across threads)
- [ ] Progress tuple tracks: `(scanned, total, created, skipped)`
- [ ] Errors during scanning are captured and available via `get_status()`

### Task 2: Add Cancellation Checkpoints

**Description**: Modify the scanning logic to check for cancellation between batches, allowing graceful early termination.

**Acceptance Criteria**:
- [ ] `threading.Event` is checked between batch operations
- [ ] When cancellation is requested, current batch completes before stopping
- [ ] Partial progress is committed (assets already scanned remain in database)
- [ ] State transitions to `IDLE` after cancellation completes
- [ ] Cancellation is logged with partial progress statistics

### Task 3: Update Startup Integration

**Description**: Modify `main.py` to use non-blocking asset seeding at startup.

**Acceptance Criteria**:
- [ ] `setup_database()` calls `asset_seeder.start(roots=["models"])` instead of `seed_assets()`
- [ ] Startup proceeds immediately without waiting for scan completion
- [ ] `asset_seeder.shutdown()` called in `finally` block alongside `cleanup_temp()`
- [ ] `--disable-assets-autoscan` flag continues to skip seeding entirely
- [ ] Startup logs indicate background scan was initiated (not completed)

### Task 4: Create API Endpoints

**Description**: Add REST endpoints for triggering, monitoring, and cancelling background scans.

**Endpoints**:

#### POST /api/assets/seed
Trigger a background scan for specified roots.

**Acceptance Criteria**:
- [ ] Accepts `{"roots": ["models", "input", "output"]}` in request body
- [ ] Returns `202 Accepted` with `{"status": "started"}` when scan begins
- [ ] Returns `409 Conflict` with `{"status": "already_running"}` if scan in progress
- [ ] Supports `?wait=true` query param for synchronous behavior (blocks until complete)
- [ ] With `?wait=true`, returns `200 OK` with final statistics on completion

#### GET /api/assets/seed/status
Get current scan status and progress.

**Acceptance Criteria**:
- [ ] Returns `{"state": "IDLE|RUNNING|CANCELLING", "progress": {...}, "errors": [...]}`
- [ ] Progress object includes: `scanned`, `total`, `created`, `skipped`
- [ ] When idle, progress reflects last completed scan (or null if never run)
- [ ] Errors array contains messages from any failures during last/current scan

#### POST /api/assets/seed/cancel
Request cancellation of in-progress scan.

**Acceptance Criteria**:
- [ ] Returns `200 OK` with `{"status": "cancelling"}` if scan was running
- [ ] Returns `200 OK` with `{"status": "idle"}` if no scan was running
- [ ] Cancellation is graceful—does not corrupt database state

### Task 5: Add WebSocket Progress Events

**Description**: Emit WebSocket events during scanning so the UI can display progress.

**Acceptance Criteria**:
- [ ] Event type: `assets.seed.started` with `{"roots": [...], "total": N}`
- [ ] Event type: `assets.seed.progress` with `{"scanned": N, "total": M, "created": C}`
- [ ] Event type: `assets.seed.completed` with final statistics
- [ ] Event type: `assets.seed.cancelled` if scan was cancelled
- [ ] Event type: `assets.seed.error` if scan failed with error message
- [ ] Progress events emitted at reasonable intervals (not every file, ~every 100 files or 1 second)

### Task 6: Update Test Helper

**Description**: Modify the test helper to use synchronous behavior via query parameter.

**Acceptance Criteria**:
- [ ] `trigger_sync_seed_assets()` uses `?wait=true` query parameter
- [ ] Tests continue to pass with synchronous blocking behavior
- [ ] Remove artificial `time.sleep(0.2)` delay (no longer needed with `wait=true`)

### Task 7: Unit Tests for AssetSeeder

**Description**: Add unit tests covering the seeder state machine and thread safety.

**Acceptance Criteria**:
- [ ] Test: `start()` transitions state from IDLE to RUNNING
- [ ] Test: `start()` while RUNNING returns False (idempotent)
- [ ] Test: `cancel()` transitions state from RUNNING to CANCELLING
- [ ] Test: `wait()` blocks until thread completes
- [ ] Test: `wait(timeout)` returns False if timeout expires
- [ ] Test: `get_status()` returns correct progress during scan
- [ ] Test: Concurrent `start()` calls are safe (only one thread spawned)
- [ ] Test: Scan commits partial progress on cancellation
- [ ] Test: Database errors are captured in status, don't crash thread

---

## Thread Safety Considerations

| Shared Resource | Protection Strategy |
|-----------------|---------------------|
| `_state` enum | Protected by `threading.Lock` |
| `_progress` tuple | Protected by `threading.Lock` |
| `_errors` list | Protected by `threading.Lock` |
| `_thread` reference | Protected by `threading.Lock` |
| `_cancel_event` | `threading.Event` (inherently thread-safe) |
| Database sessions | Created per-operation inside thread (no sharing) |

## Error Handling

- Database connection failures: Log error, set state to IDLE, populate errors list
- Individual file scan failures: Log warning, continue with next file, increment error count
- Thread crashes: Caught by outer try/except, state reset to IDLE, error captured

## Future Considerations

- **Priority queue**: If targeted scans need to be non-blocking in the future, the seeder could be extended with a priority queue
- **Persistent state**: Scan progress could be persisted to allow resume after restart
- **Parallel scanning**: Multiple threads could scan different roots concurrently (requires careful session management)
- **Throttling**: If scanning competes with generation (e.g., disk I/O contention when hashing large files), add configurable sleep between batches. Currently considered low risk since scanning is I/O-bound and generation is GPU-bound.
