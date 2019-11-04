# Changelog

## 2.2.0

- Added support for health (PR #156)
- Added support for debug (PR #155)
- Added full feature parity with PFS, PPS and transactions (PR #152, #151)
    - Added `health_records` to `put_file` methods
    - Added `input_tree_object_hash` to `finish_commit`
    - Added `reverse` `list_commit` and `list_branch`
    - Added `prov` to `subscribe_commit`
    - Added `full` to `list_job`
    - Added `spec_commit` to `create_pipeline`
    - Added `memory_bytes` to `garbage_collect`
    - Added `fsck`, `diff_file`, `delete_all_transactions`

## 2.1.0

- Added support for admin (PR #139)
- Added support for transactions (PR #138)
- Expose `spout` parameter for `create_pipeline`  (PR #137)

## 2.0.0

- Major, backwards-incompatible refactor. The largest change is to move all functionality into a single `Client` class. See this PR for more details: https://github.com/pachyderm/python-pachyderm/pull/129

## 1.9.7

- Synced with pachyderm core v1.9.7
- Note that this is the last version that will be pinned to pachyderm core versions. Future revisions will rely on semver. See the readme for details.
