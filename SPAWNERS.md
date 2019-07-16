# Notes on specific spawners

**Spawner maintainers**: Included below are "spawner maintainers",
when available.  There aren't official obligations, but the general
idea is that you should watch the repository and feel especially
empowered to comment on issues when you think it might be relevant to
you (obviously everyone should be, but this is our attempt at even
more outreach).  You should let us know when we break something and
provide a diversity of opinions in general.  Submitting PRs and
testing is nice but not required.

To be listed as a maintainer, just submit an issue or PR adding you,
and please watch the repository on Github.

## `TorqueSpawner`

Maintainers:


## `MoabSpawner`

Subclass of TorqueSpawner

Maintainers:


## `SlurmSpawner`

Maintainers: @rkdarst

This spawner enforces the environment if `srun` is used to wrap the
spawner command, which is the default.  If you *do* want user
environment to be used, set `req_srun=''`.  However, this is not
perfect: there is still a bash shell begun as the user which could run
arbitrary startup, define shell aliases for `srun`, etc.

Use of `srun` is required to gracefully terminate.


## `GridengineSpawner`

Maintainers:


## `CondorSpawner`

Maintainers:


## `LsfSpawner`

Maintainers:


# Checklist for making spawners

Please document each of these things under the spawner list above, -
even if it is "OK", we need to track status of all spawners.  If it is
a bug, users really need to know.

- Does your spawner read shell environment before starting?  (See
  [Jupyterhub
  Security](https://jupyterhub.readthedocs.io/en/stable/reference/websecurity.html).

- Does your spawner send SIGTERM to the jupyterhub-singleuser process
  before SIGKILL?  It should, so that the process can terminate
  gracefully.  Add `echo "terminated gracefully"` to the end of the
  batch script - if you see this in your singleuser server output, you
  know that you DO receive SIGTERM and terminate gracefully.  If your
  batch system can not automatically send SIGTERM before SIGKILL, PR
  #75 might help here, ask for it to be finished.

