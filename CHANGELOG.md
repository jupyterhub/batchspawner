# Changelog

## v1.3

### v1.3.0 - 2024-03-19

This release requires Python >=3.6 and JupyterHub >=1.5.1.

#### New features added

- allow for req_keepvars_extra to be configured [#295](https://github.com/jupyterhub/batchspawner/pull/295) ([@mark-tomich](https://github.com/mark-tomich), [@minrk](https://github.com/minrk))

#### Bugs fixed

- Remove `which jupyterhub-singleuser` command from `SlurmSpawner.batch_script` [#265](https://github.com/jupyterhub/batchspawner/pull/265) ([@t20100](https://github.com/t20100), [@consideRatio](https://github.com/consideRatio))

#### Maintenance and upkeep improvements

- TST: don't assume test user is OS user [#301](https://github.com/jupyterhub/batchspawner/pull/301) ([@minrk](https://github.com/minrk))
- Add python 3.12 for tests [#299](https://github.com/jupyterhub/batchspawner/pull/299) ([@Ph0tonic](https://github.com/Ph0tonic), [@consideRatio](https://github.com/consideRatio))
- maint: req py36+ and jh 1.5.1+, fix tests, add RELEASE.md, add pre-commit hooks, add dependabot [#273](https://github.com/jupyterhub/batchspawner/pull/273) ([@consideRatio](https://github.com/consideRatio), [@mbmilligan](https://github.com/mbmilligan), [@ryanlovett](https://github.com/ryanlovett), [@yuvipanda](https://github.com/yuvipanda), [@mahendrapaipuri](https://github.com/mahendrapaipuri))
- Upgrade singleuser.py to JupyterHub 4 [#267](https://github.com/jupyterhub/batchspawner/pull/267) ([@mahendrapaipuri](https://github.com/mahendrapaipuri), [@minrk](https://github.com/minrk), [@consideRatio](https://github.com/consideRatio))
- Remove reading/setting HubAuth SSL attributes in singeuser [#259](https://github.com/jupyterhub/batchspawner/pull/259) ([@cmd-ntrf](https://github.com/cmd-ntrf), [@consideRatio](https://github.com/consideRatio))
- Fix Slurm test used regular expression [#256](https://github.com/jupyterhub/batchspawner/pull/256) ([@t20100](https://github.com/t20100), [@consideRatio](https://github.com/consideRatio))
- Quell async warning, and POST with body for jupyterhub 3.0 [#247](https://github.com/jupyterhub/batchspawner/pull/247) ([@ryanlovett](https://github.com/ryanlovett), [@mbmilligan](https://github.com/mbmilligan), [@rcthomas](https://github.com/rcthomas), [@minrk](https://github.com/minrk), [@jbeal-work](https://github.com/jbeal-work), [@mawigh](https://github.com/mawigh), [@cmd-ntrf](https://github.com/cmd-ntrf), [@jaescartin1](https://github.com/jaescartin1))
- Improve submit_batch_script logging [#219](https://github.com/jupyterhub/batchspawner/pull/219) ([@cmd-ntrf](https://github.com/cmd-ntrf), [@consideRatio](https://github.com/consideRatio), [@mbmilligan](https://github.com/mbmilligan))

#### Documentation improvements

- Add temporary info about a temporary bug with JupyterHub 3+ [#290](https://github.com/jupyterhub/batchspawner/pull/290) ([@krokicki](https://github.com/krokicki), [@consideRatio](https://github.com/consideRatio))

#### Continuous integration improvements

- Modernize test matrix [#252](https://github.com/jupyterhub/batchspawner/pull/252) ([@mbmilligan](https://github.com/mbmilligan))

#### Contributors to this release

The following people contributed discussions, new ideas, code and documentation contributions, and review.
See [our definition of contributors](https://github-activity.readthedocs.io/en/latest/#how-does-this-tool-define-contributions-in-the-reports).

([GitHub contributors page for this release](https://github.com/jupyterhub/batchspawner/graphs/contributors?from=2022-10-05&to=2024-03-19&type=c))

@basnijholt ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Abasnijholt+updated%3A2022-10-05..2024-03-19&type=Issues)) | @cmd-ntrf ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Acmd-ntrf+updated%3A2022-10-05..2024-03-19&type=Issues)) | @consideRatio ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3AconsideRatio+updated%3A2022-10-05..2024-03-19&type=Issues)) | @jaescartin1 ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Ajaescartin1+updated%3A2022-10-05..2024-03-19&type=Issues)) | @jbeal-work ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Ajbeal-work+updated%3A2022-10-05..2024-03-19&type=Issues)) | @krokicki ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Akrokicki+updated%3A2022-10-05..2024-03-19&type=Issues)) | @mahendrapaipuri ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Amahendrapaipuri+updated%3A2022-10-05..2024-03-19&type=Issues)) | @mark-tomich ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Amark-tomich+updated%3A2022-10-05..2024-03-19&type=Issues)) | @mawigh ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Amawigh+updated%3A2022-10-05..2024-03-19&type=Issues)) | @mbmilligan ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Ambmilligan+updated%3A2022-10-05..2024-03-19&type=Issues)) | @minrk ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Aminrk+updated%3A2022-10-05..2024-03-19&type=Issues)) | @opoplawski ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Aopoplawski+updated%3A2022-10-05..2024-03-19&type=Issues)) | @Ph0tonic ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3APh0tonic+updated%3A2022-10-05..2024-03-19&type=Issues)) | @rcthomas ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Arcthomas+updated%3A2022-10-05..2024-03-19&type=Issues)) | @ryanlovett ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Aryanlovett+updated%3A2022-10-05..2024-03-19&type=Issues)) | @t20100 ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3At20100+updated%3A2022-10-05..2024-03-19&type=Issues)) | @yuvipanda ([activity](https://github.com/search?q=repo%3Ajupyterhub%2Fbatchspawner+involves%3Ayuvipanda+updated%3A2022-10-05..2024-03-19&type=Issues))

## v1.2

### v1.2.0 - 2022-10-04

Changed

- PR #237: Replace use of scripts with entry_points
- PR #208 #238 #239 #240 #241: updates to CI - bumping versions and aligning with Jupyterhub standards
- PR #220: remove code supporting Jupyterhub earlier than 0.9

Fixed

- PR #229: LSF jobs with multiple slots display each hostname ':' separated

## v1.1

### v1.1.0 - 2021-04-07

Added (user)

- PR #170: SlurmSpawner: add `req_gres` to specify `-go-res`.
- PR #137: GridEngineSpawner: spawner will now add the following system environment values to the spawner environment, in accordance with the Univa Admin Guide: `SGE_CELL`, `SGE_EXECD`, `SGE_ROOT`, `SGE_CLUSTER_NAME`, `SGE_QMASTER_PORT`, `SGE_EXECD_PORT`, `PATH`

Added (developer)

- PR #187: support for unknown job state

Changed

- PR #177: Fail on first error in batch script by setting `set -e` to script templates.
- PR #165: SlurmSpawner: Update template to use `--chdir` instead of `--workdir`. Users of Slurm older than 17.11 may need to revert this locally.
- PR #189: remove bashism from default script template
- PR #195: fix exception handling in run_command
- PR #198: change from Travis to gh-actions for testing
- PR #196: documentation
- PR #199: update setup.py

## v1.0

### v1.0.1 - 2020-11-04

- PR #189: batchspawner/batchspawner: Don't use `-o pipefail` in /bin/sh scripts
- PR #180: travis: Attempt to fix CI
- PR #177: Fail hard on first error in batch script
- PR #170: add 'gres' option to SlurmSpawner
- PR #165: Update batchspawner.py to use --chdir instead of --workdir
- PR #137: Grab environment variables needed for grid engine

### v1.0.0 - 2020-07-21

This release requires minimum JupyterHub 0.9 and Python 3.5.

Added (user)

- Add support for JupyterHub named servers. #167
- Add Jinja2 templating as an option for all scripts and commands. If '{{' or `{%` is used anywhere in the string, it is used as a jinja2 template.
- Add new option exec_prefix, which defaults to `sudo -E -u {username}`. This replaces explicit `sudo` in every batch command - changes in local commands may be needed.
- New option: `req_keepvars_extra`, which allows keeping extra variables in addition to what is defined by JupyterHub itself (addition of variables to keep instead of replacement). #99
- Add `req_prologue` and `req_epilogue` options to scripts which are inserted before/after the main jupyterhub-singleuser command, which allow for generic setup/cleanup without overriding the entire script. #96
- SlurmSpawner: add the `req_reservation` option. #91
- Add basic support for JupyterHub progress updates, but this is not used much yet. #86

Added (developer)

- Add many more tests.
- Add a new page `SPAWNERS.md` which information on specific spawners. Begin trying to collect a list of spawner-specific contacts. #97
- Rename `current_ip` and `current_port` commands to `ip` and `port`. No user impact. #139
- Update to Python 3.5 `async` / `await` syntax to support JupyterHub progress updates. #90

Changed

- PR #58 and #141 changes logic of port selection, so that it is selected _after_ the singleuser server starts. This means that the port number has to be conveyed back to JupyterHub. This requires the following changes:
  - `jupyterhub_config.py` _must_ explicitely import `batchspawner`
  - Add a new option `batchspawner_singleuser_cmd` which is used as a wrapper in the single-user servers, which conveys the remote port back to JupyterHub. This is now an integral part of the spawn process.
  - If you have installed with `pip install -e`, you will have to re-install so that the new script `batchspawner-singleuser` is added to `$PATH`.
- Update minimum requirements to JupyterHub 0.9 and Python 3.5. #143
- Update Slurm batch script. Now, the single-user notebook is run in a job step, with a wrapper of `srun`. This may need to be removed using `req_srun=''` if you don't want environment variables limited.
- Pass the environment dictionary to the queue and cancel commands as well. This is mostly user environment, but may be useful to these commands as well in some cases. #108, #111 If these environment variables were used for authentication as an admin, be aware that there are pre-existing security issues because they may be passed to the user via the batch submit command, see #82.

Fixed

- Improve debugging on failed submission by raising errors including error messages from the commands. #106
- Many other non-user or developer visible changes. #107 #106 #100
- In Travis CI, blacklist jsonschema=3.0.0a1 because it breaks tests

Removed

## v0.8

### v0.8.1 - 2018-05-02

- Fix regression: single-user server binding address is overwritten by previous session server address, resulting in failure to start. Issue #76

### v0.8.0 - 2018-04-24

This release is compatible with JupyterHub 0.5.0 through 0.8.1/0.9dev.

- SlurmSpawner: Remove `--uid` for (at least) Slurm 17.11 compatibility. If you use `sudo`, this should not be necessary, but because this is security related you should check that user management is as you expect. If your configuration does not use `sudo` then you may need to add the `--uid` option in a custom `batch_script`.
- add base options `req_ngpus` `req_partition` `req_account` and `req_options`
- Fix up logging
- Merge `user_options` with the template substitution vars instead of having it as a separate key
- Update ip/port handling for JupyterHub 0.8
- Add `LICENSE` (BSD3) and `CONTRIBUTING.md`
- Add `LsfSpawner` for IBM LFS
- Add `MultiSlurmSpawner`
- Add `MoabSpawner`
- Add `condorSpawner`
- Add `GridEngineSpawner`
- SlurmSpawner: add `req_qos` option
- WrapSpawner and ProfilesSpawner, which provide mechanisms for runtime configuration of spawners, have been split out and moved to the [`wrapspawner`](https://github.com/jupyterhub/wrapspawner) package
- Enable CI testing via Travis-CI

## v0.3

### v0.3.0 - 2015-11-30

- initial release containing `TorqueSpawner` and `SlurmSpawner`
