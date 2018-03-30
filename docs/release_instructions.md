# Release Instructions

1) Get the version of [pachyderm](http://github.com/pachyderm/pachyderm/releases) you want to update to

2) Checkout a release branch

e.g.

```
git checkout -b v1.2.3
```

3) Run the sync task

e.g.

```
make PACHYDERM_VERSION="1.2.3" sync
```

This will update the protos based on the latest pachyderm changes.

Commit this change **including the updated submodule** and push to origin.

4) Release

Once CI passes, assign this to an owner, who will merge the PR, and run the `make release` task.
