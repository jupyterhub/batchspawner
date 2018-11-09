# batchspawner for Jupyterhub

[![Build Status](https://travis-ci.org/jupyterhub/batchspawner.svg?branch=master)](https://travis-ci.org/jupyterhub/batchspawner)

This is a custom spawner for [Jupyterhub](https://jupyterhub.readthedocs.io/) that is designed for installations on clusters using batch scheduling software.

This began as a generalization of [mkgilbert's batchspawner](https://github.com/mkgilbert/slurmspawner) which in turn was inspired by [Andrea Zonca's blog post](http://zonca.github.io/2015/04/jupyterhub-hpc.html 'Run jupyterhub on a Supercomputer') where he explains his implementation for a spawner that uses SSH and Torque. His github repo is found [here](http://www.github.com/zonca/remotespawner 'RemoteSpawner').

This package formerly included WrapSpawner and ProfilesSpawner, which provide mechanisms for runtime configuration of spawners.  These have been split out and moved to the [`wrapspawner`](https://github.com/jupyterhub/wrapspawner) package.

## Installation
1. from root directory of this repo (where setup.py is), run `pip install -e .`

   If you don't actually need an editable version, you can simply run
      `pip install batchspawner`

2. add lines in jupyterhub_config.py for the spawner you intend to use, e.g.

   ```python
      c = get_config()
      c.JupyterHub.spawner_class = 'batchspawner.TorqueSpawner'
   ```
3. Depending on the spawner, additional configuration will likely be needed.

## Batch Spawners

For information on the specific spawners, see [SPAWNERS.md](SPAWNERS.md).

### Overview

This file contains an abstraction layer for batch job queueing systems (`BatchSpawnerBase`), and implements
Jupyterhub spawners for Torque, Moab, SLURM, SGE, HTCondor, LSF, and eventually others.
Common attributes of batch submission / resource manager environments will include notions of:
  * queue names, resource manager addresses
  * resource limits including runtime, number of processes, memory
  * singleuser child process running on (usually remote) host not known until runtime
  * job submission and monitoring via resource manager utilities
  * remote execution via submission of templated scripts
  * job names instead of PIDs

`BatchSpawnerBase` provides several general mechanisms:
  * configurable traits `req_foo` that are exposed as `{foo}` in job template scripts.  Templates (submit scripts in particular) may also use the full power of [jinja2](http://jinja.pocoo.org/).  Templates are automatically detected if a `{{` or `{%` is present, otherwise str.format() used.
  * configurable command templates for submitting/querying/cancelling jobs
  * a generic concept of job-ID and ID-based job state tracking
  * overrideable hooks for subclasses to plug in logic at numerous points

### Example

Every effort has been made to accomodate highly diverse systems through configuration
only. This example consists of the (lightly edited) configuration used by the author
to run Jupyter notebooks on an academic supercomputer cluster.

   ```python
   # Select the Torque backend and increase the timeout since batch jobs may take time to start
   c.JupyterHub.spawner_class = 'batchspawner.TorqueSpawner'
   c.Spawner.http_timeout = 120

   #------------------------------------------------------------------------------
   # BatchSpawnerBase configuration
   #    These are simply setting parameters used in the job script template below
   #------------------------------------------------------------------------------
   c.BatchSpawnerBase.req_nprocs = '2'
   c.BatchSpawnerBase.req_queue = 'mesabi'
   c.BatchSpawnerBase.req_host = 'mesabi.xyz.edu'
   c.BatchSpawnerBase.req_runtime = '12:00:00'
   c.BatchSpawnerBase.req_memory = '4gb'
   #------------------------------------------------------------------------------
   # TorqueSpawner configuration
   #    The script below is nearly identical to the default template, but we needed
   #    to add a line for our local environment. For most sites the default templates
   #    should be a good starting point.
   #------------------------------------------------------------------------------
   c.TorqueSpawner.batch_script = '''#!/bin/sh
   #PBS -q {queue}@{host}
   #PBS -l walltime={runtime}
   #PBS -l nodes=1:ppn={nprocs}
   #PBS -l mem={memory}
   #PBS -N jupyterhub-singleuser
   #PBS -v {keepvars}
   module load python3
   {cmd}
   '''
   # For our site we need to munge the execution hostname returned by qstat
   c.TorqueSpawner.state_exechost_exp = r'int-\1.mesabi.xyz.edu'
   ```

### Security

Unless otherwise stated for a specific spawner, assume that spawners
*do* evaluate shell environment for users and thus the [security
requiremnts of JupyterHub security for untrusted
users](https://jupyterhub.readthedocs.io/en/stable/reference/websecurity.html)
are not fulfilled because some (most?) spawners *do* start a user
shell which will execute arbitrary user environment configuration
(``.profile``, ``.bashrc`` and the like) unless users do not have
access to their own cluster user account.  This is something which we
are working on.


## Provide different configurations of BatchSpawner

### Overview

`ProfilesSpawner`, available as part of the [`wrapspawner`](https://github.com/jupyterhub/wrapspawner)
package, allows the Jupyterhub administrator to define a set of different spawning configurations,
both different spawners and different configurations of the same spawner.
The user is then presented a dropdown menu for choosing the most suitable configuration for their needs.

This method provides an easy and safe way to provide different configurations of `BatchSpawner` to the
users, see an example below.

### Example

The following is based on the author's configuration (at the same site as the example above)
showing how to give users access to multiple job configurations on the batch scheduled
clusters, as well as an option to run a local notebook directly on the jupyterhub server.

   ```python
   # Same initial setup as the previous example
   c.JupyterHub.spawner_class = 'wrapspawner.ProfilesSpawner'
   c.Spawner.http_timeout = 120
   #------------------------------------------------------------------------------
   # BatchSpawnerBase configuration
   #   Providing default values that we may omit in the profiles
   #------------------------------------------------------------------------------
   c.BatchSpawnerBase.req_host = 'mesabi.xyz.edu'
   c.BatchSpawnerBase.req_runtime = '12:00:00'
   c.TorqueSpawner.state_exechost_exp = r'in-\1.mesabi.xyz.edu'
   #------------------------------------------------------------------------------
   # ProfilesSpawner configuration
   #------------------------------------------------------------------------------
   # List of profiles to offer for selection. Signature is:
   #   List(Tuple( Unicode, Unicode, Type(Spawner), Dict ))
   # corresponding to profile display name, unique key, Spawner class,
   # dictionary of spawner config options.
   #
   # The first three values will be exposed in the input_template as {display},
   # {key}, and {type}
   #
   c.ProfilesSpawner.profiles = [
      ( "Local server", 'local', 'jupyterhub.spawner.LocalProcessSpawner', {'ip':'0.0.0.0'} ),
      ('Mesabi - 2 cores, 4 GB, 8 hours', 'mesabi2c4g12h', 'batchspawner.TorqueSpawner',
         dict(req_nprocs='2', req_queue='mesabi', req_runtime='8:00:00', req_memory='4gb')),
      ('Mesabi - 12 cores, 128 GB, 4 hours', 'mesabi128gb', 'batchspawner.TorqueSpawner',
         dict(req_nprocs='12', req_queue='ram256g', req_runtime='4:00:00', req_memory='125gb')),
      ('Mesabi - 2 cores, 4 GB, 24 hours', 'mesabi2c4gb24h', 'batchspawner.TorqueSpawner',
         dict(req_nprocs='2', req_queue='mesabi', req_runtime='24:00:00', req_memory='4gb')),
      ('Interactive Cluster - 2 cores, 4 GB, 8 hours', 'lab', 'batchspawner.TorqueSpawner',
         dict(req_nprocs='2', req_host='labhost.xyz.edu', req_queue='lab',
             req_runtime='8:00:00', req_memory='4gb', state_exechost_exp='')),
      ]
   ```


## Changelog

### dev (requires minimum JupyterHub 0.7.2 and Python 3.4)

Added (user)

* Add Jinja2 templating as an option for all scripts and commands.  If '{{' or `{%` is used anywhere in the string, it is used as a jinja2 template.
* Add new option exec_prefix, which defaults to `sudo -E -u {username}`.  This replaces explicit `sudo` in every batch command - changes in local commands may be needed.
* New option: `req_keepvars_extra`, which allows keeping extra variables in addition to what is defined by JupyterHub itself (addition of variables to keep instead of replacement).  #99
* Add `req_prologue` and `req_epilogue` options to scripts which are inserted before/after the main jupyterhub-singleuser command, which allow for generic setup/cleanup without overriding the entire script.  #96
* SlurmSpawner: add the `req_reservation` option.  #

Added (developer)

* Add many more tests.
* Add a new page `SPAWNERS.md` which information on specific spawners.  Begin trying to collect a list of spawner-specific contacts.  #97

Changed

* Update minimum requirements to JupyterHub 0.8.1 and Python 3.4.
* Update Slurm batch script.  Now, the single-user notebook is run in a job step, with a wrapper of `srun`.  This may need to be removed using `req_srun=''` if you don't want environment variables limited.
* Pass the environment dictionary to the queue and cancel commands as well.  This is mostly user environment, but may be useful to these commands as well in some cases. #108, #111  If these envioronment variables were used for authentication as an admin, be aware that there are pre-existing security issues because they may be passed to the user via the batch submit command, see #82.

Fixed

* Improve debugging on failed submission by raising errors including error messages from the commands.  #106
* Allow selecting random ports via different logic, for example select a random port only within a certain range via: `c.BatchSpawner.random_port = batchspawner.utils.random_port_range(low, high)`.
* Many other non-user or developer visible changes.  #107 #106 #100
* In Travis CI, blacklist jsonschema=3.0.0a1 because it breaks tests

Removed


### v0.8.1 (bugfix release)

* Fix regression: single-user server binding address is overwritten by previous session server address, resulting in failure to start.  Issue #76

### v0.8.0 (compatible with JupyterHub 0.5.0 through 0.8.1/0.9dev)

* SlurmSpawner: Remove `--uid` for (at least) Slurm 17.11 compatibility.  If you use `sudo`, this should not be necessary, but because this is security related you should check that user management is as you expect.  If your configuration does not use `sudo` then you may need to add the `--uid` option in a custom `batch_script`.
* add base options `req_ngpus` `req_partition` `req_account` and `req_options` 
* Fix up logging
* Merge `user_options` with the template substitution vars instead of having it as a separate key
* Update ip/port handling for JupyterHub 0.8
* Add `LICENSE` (BSD3) and `CONTRIBUTING.md`
* Add `LsfSpawner` for IBM LFS
* Add `MultiSlurmSpawner`
* Add `MoabSpawner`
* Add `condorSpawner`
* Add `GridEngineSpawner`
* SlurmSpawner: add `req_qos` option
* WrapSpawner and ProfilesSpawner, which provide mechanisms for runtime configuration of spawners, have been split out and moved to the [`wrapspawner`](https://github.com/jupyterhub/wrapspawner) package
* Enable CI testing via Travis-CI


### v0.3 (tag: jhub-0.3, compatible with JupyterHub 0.3.0)

* initial release containing `TorqueSpawner` and `SlurmSpawner`

