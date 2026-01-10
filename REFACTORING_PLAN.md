# ZFS API Clean Architecture Refactoring Plan

## Project Analysis Completed: 2026-01-10

---

## Current State Analysis

### Code Statistics
- **zfs.py**: 61 methods across 3 classes (zfs, zpool, zdb)
- **zfs_api_server.py**: 41 JSON-RPC methods
- **Implementation patterns**: 2 different approaches (async_wrapper + direct subprocess)
- **Technical debt**: Thread pool executor, duplicated code, inconsistent error handling

### Problems Identified
1. **Dual execution patterns**: Some operations via zfs.py wrapper, others via direct subprocess
2. **Inconsistent error handling**: Return codes vs stderr capture
3. **No centralized sudo support**: Would require 12+ places to modify
4. **Duplicated code**: get_space() defined twice, zdb duplicates zpool methods
5. **Missing operations**: Many zfs.py methods not exposed in API

---

## Target Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────┐
│  Layer 1: Command Builder (Pure Logic)             │
│  - Single source of truth for all ZFS commands      │
│  - No execution, just command construction          │
└─────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────┬──────────────────────────┐
│  Layer 2a: AsyncZFS      │  Layer 2b: SyncZFS       │
│  - asyncio execution     │  - subprocess execution  │
│  - For API server        │  - For scripts/tools     │
│  - Same method API       │  - Same method API       │
└──────────────────────────┴──────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│  Layer 3: OS Execution                              │
│  - asyncio.create_subprocess_exec (async)           │
│  - subprocess.Popen (sync)                          │
└─────────────────────────────────────────────────────┘
```

---

## Operations to Implement

### Total: 47 Operations Across 8 Categories

1. **Dataset Operations** (11): create, destroy, list, get_properties, set_property, get_space, mount, rename, promote, share, unshare
2. **Snapshot Operations** (10): create, list, destroy, rollback, hold, release, list_holds, diff, create_auto, autoremove
3. **Pool Operations** (8): list, get_properties, set_property, scrub_start, scrub_stop, status, import, export
4. **Bookmark Operations** (3): create, list, destroy
5. **Clone Operations** (1): create
6. **Volume Operations** (3): create, list, destroy
7. **Send/Receive Operations** (6): send, receive, send_estimate, send_to_file, receive_from_file, negotiate_incremental
8. **Diagnostic Operations** (5): get_pool_state, get_operation_progress, check_exists, get_version

---

## Implementation Phases

### Phase 1: Command Builder (6 hours) - CRITICAL
**File**: `zfs_commands/builder.py`
- Pure Python command construction
- All 47 operations
- No execution logic
- Returns List[str] commands

### Phase 2: Async Executor (4 hours) - HIGH
**File**: `zfs_commands/async_executor.py`
- AsyncZFS class
- Native asyncio.create_subprocess_exec
- All 47 operations as async methods
- Consistent CommandResult return type

### Phase 3: Sync Executor (3 hours) - HIGH
**File**: `zfs_commands/sync_executor.py`
- SyncZFS class
- subprocess.Popen execution
- EXACT same API as AsyncZFS
- For standalone scripts

### Phase 4.1: Migrate API Server (4 hours) - HIGH
**File**: `zfs_api_server.py`
- Replace async_wrapper pattern with AsyncZFS
- Remove ThreadPoolExecutor
- Consolidate all subprocess calls
- Better error handling with stderr

### Phase 4.2: Migrate standalone_migrate.py (2 hours) - MEDIUM
- Use SyncZFS instead of custom execute()
- Cleaner code, less duplication

### Phase 4.3: Migrate socket servers (1 hour) - MEDIUM
- Use ZFSCommands for command building
- Keep Popen for streaming

### Phase 5: Testing (4 hours) - HIGH
- Unit tests for command builder
- Integration tests for executors
- API compatibility tests

---

## Timeline

**Total Effort**: 24 hours
**Priority Focus**: Phases 1-4.1 (17 hours)

---

## Success Criteria

✅ Single source of truth for ZFS commands
✅ Async and sync with identical APIs
✅ No thread pool needed
✅ Consistent error handling
✅ All 47 operations implemented
✅ Better error messages (stderr)
✅ Easy sudo support (modify 1 place)
✅ Deprecate zfs.py completely

---

## Benefits

### Before
- 4 files executing ZFS commands
- 2 different patterns
- 12+ places to modify for sudo
- Inconsistent errors
- Thread pool overhead

### After
- 1 command builder
- 2 executors (async + sync, same API)
- 1 place to modify for sudo
- Consistent CommandResult
- Native async, no threads

---

## Migration Path

1. ✅ Create git repository
2. 🔄 Implement new architecture
3. Migrate one method at a time
4. Test incrementally
5. Deprecate old code
6. Remove thread executor

**Status**: Ready to begin implementation
