#batchspawner for Jupyterhub
This is a custom spawner for Jupyterhub that is designed for installations on clusters using batch scheduling software.

This began as a generalization of [mkgilbert's batchspawner](https://github.com/mkgilbert/slurmspawner) which in turn was inspired by [Andrea Zonca's blog post](http://zonca.github.io/2015/04/jupyterhub-hpc.html 'Run jupyterhub on a Supercomputer') where he explains his implementation for a spawner that uses SSH and Torque. His github repo is found [here](http://www.github.com/zonca/remotespawner 'RemoteSpawner').

##Installation
1. from root directory of this repo (where setup.py is), run `pip install -e .`
2. add lines in jupyterhub_config.py for the spawner you intend to use, e.g.
   
   ```python
      c = get_config()
      c.JupyterHub.spawner_class = 'batchspawner.TorqueSpawner'
   ```
3. Depending on the spawner, additional configuration will likely be needed.
