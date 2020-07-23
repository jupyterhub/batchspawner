# Changelog

## unreleased changes

Added (user)

* PR #170: SlurmSpawner: add `req_gres` to specify `-go-res`.
* PR #137: GridEngineSpawner: spawner will now add the following system environment values to the spawner environment, in accordance with the Univa Admin Guide: `SGE_CELL`, `SGE_EXECD`, `SGE_ROOT`, `SGE_CLUSTER_NAME`, `SGE_QMASTER_PORT`, `SGE_EXECD_PORT`, `PATH`

Added (developer)

Changed

* PR #177: Fail on first error in batch script by setting `set -e` to script templates.
* PR #165: SlurmSpawner: Update template to use `--chdir` instead of `--workdir`. Users of Slurm older than 17.11 may need to revert this locally.

Fixed


## v1.0 (requires minimum JupyterHub 0.9 and Python 3.5)

Added (user)

* Add support for JupyterHub named servers. #167
* Add Jinja2 templating as an option for all scripts and commands.  If '{{' or `{%` is used anywhere in the string, it is used as a jinja2 template.
* Add new option exec_prefix, which defaults to `sudo -E -u {username}`.  This replaces explicit `sudo` in every batch command - changes in local commands may be needed.
* New option: `req_keepvars_extra`, which allows keeping extra variables in addition to what is defined by JupyterHub itself (addition of variables to keep instead of replacement).  #99
* Add `req_prologue` and `req_epilogue` options to scripts which are inserted before/after the main jupyterhub-singleuser command, which allow for generic setup/cleanup without overriding the entire script.  #96
* SlurmSpawner: add the `req_reservation` option.  #91
* Add basic support for JupyterHub progress updates, but this is not used much yet.  #86

Added (developer)

* Add many more tests.
* Add a new page `SPAWNERS.md` which information on specific spawners.  Begin trying to collect a list of spawner-specific contacts.  #97
* Rename `current_ip` and `current_port` commands to `ip` and `port`.  No user impact.  #139
* Update to Python 3.5 `async` / `await` syntax to support JupyterHub progress updates.  #90

Changed

* PR #58 and #141 changes logic of port selection, so that it is selected *after* the singleuser server starts.  This means that the port number has to be conveyed back to JupyterHub.  This requires the following changes:
  - `jupyterhub_config.py` *must* explicitely import `batchspawner`
  - Add a new option `batchspawner_singleuser_cmd` which is used as a wrapper in the single-user servers, which conveys the remote port back to JupyterHub.  This is now an integral part of the spawn process.
  - If you have installed with `pip install -e`, you will have to re-install so that the new script `batchspawner-singleuser` is added to `$PATH`.
* Update minimum requirements to JupyterHub 0.9 and Python 3.5.  #143
* Update Slurm batch script.  Now, the single-user notebook is run in a job step, with a wrapper of `srun`.  This may need to be removed using `req_srun=''` if you don't want environment variables limited.
* Pass the environment dictionary to the queue and cancel commands as well.  This is mostly user environment, but may be useful to these commands as well in some cases. #108, #111  If these environment variables were used for authentication as an admin, be aware that there are pre-existing security issues because they may be passed to the user via the batch submit command, see #82.


Fixed

* Improve debugging on failed submission by raising errors including error messages from the commands.  #106
* Many other non-user or developer visible changes.  #107 #106 #100
* In Travis CI, blacklist jsonschema=3.0.0a1 because it breaks tests

Removed


## v0.8.1 (bugfix release)

* Fix regression: single-user server binding address is overwritten by previous session server address, resulting in failure to start.  Issue #76

## v0.8.0 (compatible with JupyterHub 0.5.0 through 0.8.1/0.9dev)

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


## v0.3 (tag: jhub-0.3, compatible with JupyterHub 0.3.0)

* initial release containing `TorqueSpawner` and `SlurmSpawner`

