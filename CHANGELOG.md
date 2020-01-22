# Changelog

## 3.0.0

- Support for pachyderm v1.9.11 (PR #170)
    - Removed `scale_down_threshold` from `Client.create_pipeline`
    - Added `job_id` to `Client.run_pipeline`
    - Added `Client.run_cron`
    - Added `Client.batch_transaction`

## 2.6.0

- Faster `put_files` by putting multiple files in a single request (PR #166)

## 2.5.0

- Added a utility function for creating a pipeline from locally stored python code (PR #163)
- Added a utility function for recursively putting many files into PFS (PR #163)
- Removed support for python 3.4 (PR #162)

## 2.4.1

- Re-introduced `PACH_PYTHON_AUTH_TOKEN`, as we need it for JupyterHub integration (PR #161)

## 2.4.0

- Synced with pachyderm core v1.9.8 (PR #159)

## 2.3.0

- Tweaks to the way `Client`s can be initialized (PR #143, #157)
    - Removed support for the deprecated `PACHD_ADDRESS` environment variable
    - Removed support for library-specific `PACH_PYTHON_AUTH_TOKEN` environment variable
    - Added `Client.new_in_cluster`, which can be used to create a `Client` instance running inside a Pachyderm cluster
    - Added `Client.new_from_pachd_address`, which can be used to create a `Client` instance from a pachd address/URL
    - Added support for using default system certs
    - Added support for client initialization with a transaction ID
- Added support for the enterprise service (PR #157)
- Added support for the auth service (PR #157)
- Added `auth_token` and `transaction_id` properties for better ergonomics (PR #157)
- Renamed `Client.metadata` to `Client._metadata`, since it should be private (PR #157)
- Some changes to `Client.transaction` to be more ergonomic and less redundant with other functionality (PR #157)

## 2.2.0

- Added support for health (PR #156)
- Added support for debug (PR #155)
- Added full feature parity with PFS, PPS and transactions (PR #152, #151)
    - Added `health_records` to `put_file` methods
    - Added `input_tree_object_hash` to `finish_commit`
    - Added `reverse` to `list_commit` and `list_branch`
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
