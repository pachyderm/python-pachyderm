# Contributor guide

## Getting started

Setup & initialize virtualenv, for example:

```bash
python3 -m venv venv
source venv/bin/activate
```

Run the init script which pulls submodules as well as sets up the Python project and install development tools locally:

```bash
make init
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

We use `black` as our Python code formatter. After running `make init`,
there should be a pre-commit hook that runs `black` and `flake8` automatically.
You can also check that your code changes match the expected style by running `make lint` locally.
The linter will also run in CI and fail if there are any stylistic discrepancies.

### Rebuilding protobuf code

To rebuild protobuf code:

* Update `version.json` to reference the version of Pachyderm you want to pull

```bash
make src/python_pachyderm/proto/v2
```

## Testing

### Full test suite

To execute the full test suite:

* Install `tox`
* Start the cluster to run on `localhost:30650` -- if the cluster is not
exposed on `localhost`, you can use `pachctl port-forward` to proxy
connections.
* From the repo root, run `tox`.

Note that CI will still be more comprehensive than a locally executing full
test suite, because it tests several variants of python and pachyderm.

### Run one-off tests

The full test suite takes a long time to run, and will be run anyway in CI, so
locally it's usually more convenient to run specific tests. To do so:

* Setup & initialize virtualenv if you haven't done so already
* Alternatively, you can use `tox` to create a virtual environment for you:
  ```
  mkdir venvdir
  tox --devenv venvdir -e py38 # one possible environment
  source ./venvdir/bin/activate # activate python environment
  ```
* Start the cluster to run on `localhost:30650` -- if the cluster is not
exposed on `localhost`, you can use `pachctl port-forward` to proxy
connections.
* Run the test: `python3 -m pytest tests -k <test name>`

### Linting

To run the linter locally, run

```bash
make lint
```

## Rebuilding API docs

We use Sphinx to generate our API docs site.

**If you added any new files or modules** that you want included in the docs
site, you must first create the RST files for them so Sphinx can reference and
include them. To create these RST files, run:

```bash
sphinx-apidoc -o source ../src/
```

You'll see the new RST file(s) in `docs/source`. Make sure to add the reference
to these new file(s) in the other RST files.

Additionally, add any new libraries to `docs/requirements.txt` that are
needed to build the docs.

To rebuild the docs, run:

```bash
make docs
```

This is a great way to check locally that the docs look as you expect them
before you make a commit or release.

## Releasing

To make a new release, from the master branch:

* Rebuild docs to make sure they're in sync
* Update `CHANGELOG.md` and `version.json`

```bash
make release
```
