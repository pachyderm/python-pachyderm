# Changelog

## 7.1.1 (2022-02-04)
- Update protobuf files to v2.0.5
- Fix bug where only 20MiB of data could be retrieved with `client.get_file`

## 7.1.0 (2021-11-18)

- New experimental client (available via `import python-pachyderm.experimental`, which will contain beta/unstable APIs and features (#352)
- New `Mount()` API in the experimental client, which runs `pachctl mount` in a subprocess to expose pachyderm data on the local filesystem (#353)

## 7.0.0 (2021-11-01)

- Improved documentation

## 7.0.0-rc.1

- Parity with Pachyderm 2.0.0, including support for Global IDs, and corresponding API changes
- Support for Pachyderm's new spout semantics (added in Pachyderm 1.12)
  - Spout-manager has been consolidated into core client, as it's no longer necessary with this change.
- Remove returns from methods whose RPC calls return a negligible protobuf (#328)
- `put_files` supports putting individual files
- `Client()` constructor reads connection settings from the Pachyderm config
- API methods that take a commit now accept tuples of the form `(repo, branch, commit_id, repo_type)`
- Support Pachyderm root tokens
- Updated examples

## 6.1.0

- Support for constructing a `Client` from a Pachyderm config file (PR #220)
- Support for pachyderm v1.11.2 (PR #213)
    - Tweaks to debug service functions
- Support for build-step enabled pipelines (PR #213)
- Switched `create_python_pipeline` to use build-step enabled pipelines (PR #213)
- Add support for authenticating with OIDC ID tokens (PR #228)
- Clients automatically authenticate with an ID token in the PACH_PYTHON_OIDC_TOKEN env var (PR #236)
- Add the metadata field to the response from create_pipeline (PR #234)
- Add new_from_config to construct a client from a pachctl config file (PR #220)

## 6.0.0

- Changes to the interface of enums dynamically generated from protos (PR #207)
- Changes to the `SpoutManager` interface to better support commits (PR #206)
- Added support for putting or deleting many PFS files in an atomic commit (PR #204)
- Deprecated `Client.put_file_bytes` with an iterable of bytestrings (PR #204)
- Added version number to build (PR #203)

## 5.0.0

- Support for pachyderm v1.11 (PR #201)
    - Deprecated `Client.get_admins` and `Client.modify_admins`
    - Added support getting/setting cluster role bindings
    - Added support for OIDC login
    - Added the `full` flag to `Client.inspect_job`
    - Added `sidecar_resource_limits` for `Client.list_job`
    - Added Loki backend query support for log fetch functions
- Better support for working with pipeline spec files (PR #200)
    - Added `parse_json_pipeline_spec` and `parse_dict_pipeline_spec` for parsing JSON spec files
    - Added `create_pipeline_from_request` for creating a pipeline from a parsed pipeline spec
- Use `sh` rather than `bash` in python pipelines for better compatibility with custom images (PR #196)

## 4.2.0

- Allow for customization of spout directory for testing (PR #188)
- Fixed `put_file_bytes` failing for large files (PR #189)

## 4.1.0

- Support file-like operations on `get_file` results (PR #187)

## 4.0.0

- Support for pachyderm v1.10 (PR #178)
    - Added support for specifying pipeline metadata
    - Added support for s3 sidecar instances
    - Added support for secrets
- First-class support for building spout producer and consumer pipelines (PR #173, #178)

## 3.1.0

- Use pachd peer service where available in `Client.new_in_cluster`, to support TLS-enabled clusters (PR #176)

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
