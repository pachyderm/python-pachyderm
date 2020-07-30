# Contributor guide

## Getting started

When you first clone this repo, make sure to pull submodules as well:

```bash
git submodule update --init
```

## Code

### Layout

Code layout, as of 7/2020:

```
.
├── docs - Auto-generated API docs
├── etc
│   └── proto_lint
│       └── proto_lint.py - Linter to ensure we support all functionality
├── examples - The examples
│   ├── jupyter/ - Uses python_pachyderm for analysis from a locally running Jupyter instance
│   ├── opencv/ - The canonical OpenCV demo rewritten to use python_pachyderm
│   └── spout/ - A demo of spout management from python_pachyderm running in a pipeline
├── proto - Tooling for building protobuf code
│   ├── Dockerfile - Dockerfile for the pachyderm protobuf builder
│   ├── pachyderm/ - Git submodule reference the pachyderm version we're pulling protobufs from
│   ├── requirements.txt - Pip requirements for the Pachyderm protobuf builder
│   └── run - Script runner for the Pachyderm protobuf builder
├── src
│   ├── python_pachyderm
│   │   ├── __init__.py - Library entrypoint, contains magic to reduce import boilerplate
│   │   ├── client.py - The higher-level `Client` class
│   │   ├── mixin/ - Higher-level `Client` functionality, broken down by Pachyderm service
│   │   ├── proto/ - The auto-generated protobuf/gRPC code
│   │   ├── service.py - The `Service` enum, used internally for referencing Pachyderm services in function calls
│   │   ├── spout.py - The `SpoutManager`, used to help author spout pipelines
│   │   ├── util.py - Utility functions
│   │   └── version.py - Auto-generated module to expose the version of this library
├── tests/ - Pytests
├── tox.ini - Config for running tox tests (locally or in CI)
└── version.json - Spec for the version of this library, as well as its pachyderm dependency
```

### Style

We follow PEP-8, with slightly loosened max line lengths. You can check that
your code changes match the expected style by running `make lint` locally.
The linter will also run in CI and fail if there are any stylistic
discrepancies.

### Rebuilding protobuf code

To rebuild protobuf code:

* Remove the existing auto-generated code (`src/python_pachyderm/proto`)
* Update `version.json` to reference the version of Pachyderm you want to pull
* Run `make src/python_pachyderm/proto`

## Rebuilding API docs

We use [pdoc](https://github.com/mitmproxy/pdoc) for API documentation. It
sadly doesn't see much maintenance though, so [I made a fork with a branch
that has some fixes.](https://github.com/ysimonson/pdoc/tree/sandbox) Install
that version of pdoc locally.

Once installed, to rebuild API documentation:

* Remove the `docs` directory.
* Run `make docs`. Note that this should be run outside of a virtualenv, and
it will globally install `python_pachyderm`. This is needed because pdoc
treats virtualenv vs globally installed packages differently.

## Releasing

To make a new release, from the master branch:

* Rebuild docs to make sure they're in sync
* Update `CHANGELOG.md` and `version.json`
* Run `make release`
