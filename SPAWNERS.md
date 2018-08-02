# Notes on specific spawners

## `TorqueSpawner`

Maintainers:


## `MoabSpawner`

Subclass of TorqueSpawner

Maintainers:


## `SlurmSpawner`

Maintainers: @rkdarst

This spawner enforces the environment if `srun` is used, which is the
default.  If you *do* want user environment to be used, set
`req_srun=''`.


## `GridengineSpawner`

Maintainers:


## `CondorSpawner`

Maintainers:


## `LsfSpawner`

Maintainers:


# Checklist for making spawners

- Does your spawner read shell environment before starting?

- Does your spawner send SIGTERM to the jupyterhub-singleuser process
  before SIGKILL?  It should, so that the process can terminate
  gracefully.  If you don't see the script end (e.g. you can add `echo
  "terminated gracefully"` to the end of your script and see it), you
  should check.  PR #75 might help here, ask the poster to finalize
  it.

