# Contributor guide

## Getting started

We use [poetry](https://python-poetry.org/) to both manage dependencies
  and publish this package. You can find information about installing this
  software [here](https://python-poetry.org/docs/).


Once both `poetry` is installed, you can use `poetry` to create your
  virtual environment for this project. The following command
  (run at the root of this project) will create a virtual environment
  within the `.venv` directory:
```bash
poetry install
source .venv/bin/activate
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
└── version.json - Spec for the version of its pachyderm and kubernetes dependencies
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

We use [pdoc3](https://github.com/pdoc3/pdoc) to generate our API docs site.
To rebuild the docs, run

```bash
make docs
```

## Releasing

Our current process for publishing a release of python-pachyderm consists of the following steps:

#### Validate the release
- [ ] Ensure that `pyproject.toml` contains the version to be released under the `version` attribute.
  - If the `version` attribute contains an already-released version of python-pachyderm, update it in a PR.
- [ ] Ensure the protobufs are up-to-date
  - Make sure `version.json` references the most recent compatible release of Pachyderm
  - Run `make src/python_pachyderm/proto/v2`, and make sure that the generated code is unchanged
  - If it's outdated, update it in a PR
- [ ] Ensure the docs are up-to-date
  - Rebuild the docs (with `make docs`), and make sure the generated docs are unchanged
  - Merge any changes in a PR
- [ ] Ensure `CHANGELOG.md` is up-to-date
  - Commit any additional change notes in a PR.

#### Test everything
This is mostly necessary for major releases, but it always reduces risk.
  - [ ] Deploy the version of pachyderm that matches `version.json` (the latest compatible release) and run our test suite.
    - Make sure to install a matching version of `pachctl`, as e.g. python-pachyderm's `Mount()` implementation depends on `pachctl`
  - [ ] Run `make lint` and ensure there are no errors
  - [ ] Ensure that all examples still pass:
    - [ ] All examples in the `examples/` dir (they must be run manually—`tox example` only runs `examples/opencv`)
    - [ ] [Pachyderm's spouts101 example](github.com/pachyderm/pachyderm/tree/master/examples/spouts101), which should match `examples/spouts101` in this repo, but it's good to confirm

#### Make Pypi release
  - [ ] Run `make test-release`, which will checkout the release branch, build a package, and push it to test-pypi.
    - Proofread the release page, as pypi doesn't allow you to modify a release after pushing it.
  - [ ] Run `make release` which will re-build python-pachyderm and push a final version to pypi.

#### Make GitHub release
  - [ ] Go to http://github.com/pachyderm/python-pachyderm and create a new GitHub release pointing at the Git commit that was just pushed to pypi
    - Include any notes added to `CHANGELOG.md` for this release
  - [ ] &#40;Only when releasing from `master`&#41; Create a new branch for patch releases called `vA.B.x`. For example, if releasing 1.0.0, create a new `v1.0.x` branch to hold future patch-sized changes made to 1.0.0.

#### Update versions for next release
  - [ ] In the patch release branch (e.g. `v1.0.x`), create a PR to update the python-pachyderm version in `pyrpoject.toml` to contain the next _patch_ release (e.g. 1.0.1)
    - This is the python-pachyderm version that is currently in development in that branch.
  - [ ] &#40;Only when releasing from `master`&#41; In `master`, create a PR to update the python-pachyderm version in `pyrpoject.toml` to contain the next _minor_ release (e.g. 1.1.0)
    - This is the python-pachyderm version that is currently in development in `master`.
  - [ ] &#40;Only when releasing from `master`&#41; In the new patch release branch (e.g. `v1.0.x`):
    - Create a PR (or add to the above PR) to update `make release` and `make-test-release` so that it checks out `v1.0.x` instead of `master` when releasing from this branch.
    - Go to the python-pachyderm Read the Docs (RTD) page and activate the [newly created patch release branch](https://readthedocs.org/projects/python-pachyderm/versions/). This will serve up that branch's docs on RTD.
    - Update the docs link in `README.md` to point to the branch's RTD page.
    - Update the examples directory link to point to the `examples/` directory of that Github branch.
