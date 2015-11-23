# Copyright (c) Regents of the University of Minnesota
# Copyright (c) Michael Gilbert
# Distributed under the terms of the Modified BSD License.

"""Batch spawners

This file contains an abstraction layer for batch job queueing systems, and implements
Jupyterhub spawners for Torque, SLURM, and eventually others.

Common attributes of batch submission / resource manager environments will include notions of:
  * queue names, resource manager addresses
  * resource limits including runtime, number of processes, memory
  * singleuser child process running on (usually remote) host not known until runtime
  * job submission and monitoring via resource manager utilities
  * remote execution via submission of templated scripts
  * job names instead of PIDs
"""
import signal
import errno
import pwd
import os
import getpass
import time
import pipes
from subprocess import Popen, call
import subprocess
from string import Template
import tempfile

from tornado import gen

from jupyterhub.spawner import Spawner
from traitlets import (
    Instance, Integer, Unicode
)

from jupyterhub.utils import random_port
from jupyterhub.spawner import set_user_setuid

def run_command(cmd, input=None, env=None):
    popen = subprocess.Popen(cmd, shell=True, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    if input: inbytes = input.encode()
    out = popen.communicate(input=inbytes)
    if out[1] is not None:
        return out[1] # exit error?
    else:
        out = out[0].decode().strip()
        return out

class BatchSpawnerBase(Spawner):
    """Base class for spawners using resource manager batch job submission mechanisms

    This base class is developed targetting the TorqueSpawner and SlurmSpawner, so by default
    assumes a qsub-like command that reads a script from its stdin for starting jobs,
    a qstat-like command that outputs some data that can be parsed to check if the job is running
    and on what remote node, and a qdel-like command to cancel a job. The goal is to be
    sufficiently general that a broad range of systems can be supported with minimal overrides.

    At minimum, subclasses should provide reasonable defaults for the traits:
        batch_script
        batch_submit_cmd
        batch_query_cmd
        batch_cancel_cmd

    and must provide implementations for the methods:
        state_ispending
        state_isrunning
        state_gethost
    """

    # override default since batch systems typically need longer
    start_timeout = Integer(300, config=True)

    # override default server ip since batch jobs normally running remotely
    ip = Unicode("0.0.0.0", config=True, help="Address for singleuser server to listen at")

    # all these req_foo traits will be available as substvars for templated strings
    req_queue = Unicode('', config=True, \
        help="Queue name to submit job to resource manager"
        )

    req_host = Unicode('', config=True, \
        help="Host name of batch server to submit job to resource manager"
        )

    req_memory = Unicode('', config=True, \
        help="Memory to request from resource manager"
        )

    req_nprocs = Unicode('', config=True, \
        help="Number of processors to request from resource manager"
        )

    req_runtime = Unicode('', config=True, \
        help="Length of time for submitted job to run"
        )

    req_username = Unicode()
    def _req_username_default(self):
        return self.user.name

    req_keepvars = Unicode()
    def _req_keepvars_default(self):
        return ','.join(self.env.keys())

    batch_script = Unicode('', config=True, \
        help="Template for job submission script. Traits on this class named like req_xyz "
             "will be substituted in the template for {xyz} using string.Formatter. "
             "Must include {cmd} which will be replaced with the jupyterhub-singleuser command line."
        )

    # Raw output of job submission command unless overridden
    job_id = Unicode()

    # Will get the raw output of the job status command unless overridden
    job_status = Unicode()

    # Prepare substitution variables for templates using req_xyz traits
    def get_req_subvars(self):
        reqlist = [ t for t in self.trait_names() if t.startswith('req_') ]
        subvars = {}
        for t in reqlist:
            subvars[t[4:]] = getattr(self, t)
        return subvars

    batch_submit_cmd = Unicode('', config=True, \
        help="Command to run to submit batch scripts. Formatted using req_xyz traits as {xyz}."
        )

    def submit_batch_script(self):
        subvars = self.get_req_subvars()
        cmd = self.batch_submit_cmd.format(subvars)
        subvars['cmd'] = self.cmd + self.get_args()
        script = self.batch_script.format(subvars)
        self.log.info('Spawner submitting job using ' + cmd)
        out = run_command(cmd, input=script, env=self.env)
        try:
            self.job_id = out
        except:
            self.log.error('Job submission failed with exit code ' + out)
            self.job_id = ''
        return self.job_id

    # Override if your batch system needs something more elaborate to read the job status
    batch_query_cmd = Unicode('', config=True, \
        help="Command to run to read job status. Formatted using req_xyz traits as {xyz} "
             "and self.job_id as {job_id}."
        )

    def read_job_state(self):
        if self.job_id is None or len(self.job_id) == 0:
            # job not running
            self.job_status = ''
            return self.job_status
        subvars = get_req_subvars()
        subvars['job_id'] = self.job_id
        cmd = self.batch_query_cmd.format(**subvars)
        self.log.info('Spawner querying job: ' + cmd)
        try:
            out = run_command(cmd)
            self.job_status = out
        except:
            self.log.error('Error querying job ' + self.job_id)
            self.job_status = ''
        return self.job_status

    batch_cancel_cmd = Unicode('', config=True \,
        help="Command to stop/cancel a previously submitted job. Formatted like batch_query_cmd."
        )

    def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars['job_id'] = self.job_id
        cmd = self.batch_cancel_cmd.format(**subvars)
        self.log.info('Cancelling job ' + self.job_id + ': ' + cmd)
        run_command(cmd)

    def load_state(self, state):
        """load job_id from state"""
        super(BatchSpawnerBase, self).load_state(state)
        self.job_id = state.get('job_id', '')
        self.job_status = state.get('job_status', '')

    def get_state(self):
        """add job_id to state"""
        state = super(BatchSpawnerBase, self).get_state()
        if self.job_id:
            state['job_id'] = self.job_id
        if self.job_status:
            state['job_status'] = self.job_status
        return state

    def clear_state(self):
        """clear job_id state"""
        super(BatchSpawnerBase, self).clear_state()
        self.job_id = ""
        self.job_status = ''

    def make_preexec_fn(self, name):
        """make preexec fn to change uid (if running as root) before job submission"""
        return set_user_setuid(name)

    def state_ispending(self):
        "Return boolean indicating if job is still waiting to run, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    def state_isrunning(self):
        "Return boolean indicating if job is running, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    def state_gethost(self):
        "Return string, hostname or addr of running job, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    @gen.coroutine
    def poll(self):
        """Poll the process"""
        if self.job_id is not None and len(self.job_id) > 0:
            self.read_job_state()
            if self.state_isrunning() or self.state_ispending():
                return None
            else:
                self.clear_state()
                return 1

        if not self.job_id:
            # no job id means it's not running
            self.clear_state()
            return 1

class TorqueSpawner(BatchSpawnerBase):
    batch_script = Unicode("""#!/bin/sh
#PBS -q {queue}@{host}
#PBS -l walltime={runtime}
#PBS -l nodes=1:ppn={nprocs}
#PBS -l mem={memory}
#PBS -N jupyterhub-singleuser
#PBS -v {keepvars}

{cmd}
""",
        config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('qsub', config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('qstat -x {job_id}', config=True)
    batch_cancel_cmd = Unicode('qdel {job_id}', config=True)

class UserEnvMixin:
    """Mixin class that computes values for USER and HOME in the environment passed to
    the job submission subprocess in case the batch system needs these for the batch script."""

    def user_env(self, env):
        """get user environment"""
        env['USER'] = self.user.name
        env['HOME'] = pwd.getpwnam(self.user.name).pw_dir
        return env

    def _env_default(self):
        env = super()._env_default()
        return self.user_env(env)

class SlurmSpawner(BatchSpawnerBase,UserEnvMixin):
    """A Spawner that just uses Popen to start local processes."""

    batch_script = Unicode("""#!/bin/bash
#SBATCH --partition={queue}
#SBATCH --time={runtime}
#SBATCH -o /home/{username}/jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
#SBATCH --workdir=/home/$user
#SBATCH --mem={memory}
#SBATCH --export={keepvars}
#SBATCH --uid={username}
#SBATCH --get-user-env=L

which jupyterhub-singleuser
{cmd}
""",
        config=True)
    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode('sbatch', config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode('squeue -h -j {job_id} -o "%T %B"', config=True) #
    batch_cancel_cmd = Unicode('scancel {job_id}', config=True)

    def _check_slurm_job_state(self):
        if self.job_id in (None, ""):
            # job has been cancelled or failed, so don't even try the squeue command. This is because
            # squeue will return RUNNING if you submit something like `squeue -h -j -o %T` and there's
            # at least 1 job running
            return ""
        # check sacct to see if the job is still running
        cmd = 'squeue -h -j ' + self.job_id + ' -o %T'
        out = run_command(cmd)
        self.log.info("Notebook server for user %s: Slurm jobid %s status: %s" % (self.user.name, self.job_id, out))
        return out

    @gen.coroutine
    def start(self):
        """Start the process"""
        self.user.server.port = random_port()
        cmd = []
        env = self.env.copy()

        cmd.extend(self.cmd)
        cmd.extend(self.get_args())

        self.log.debug("Env: %s", str(env))
        self.log.info("Spawning %s", ' '.join(cmd))
        for k in ["JPY_API_TOKEN"]:
            cmd.insert(0, 'export %s="%s";' % (k, env[k]))
        #self.pid, stdin, stdout, stderr = execute(self.channel, ' '.join(cmd))

        output = run_jupyterhub_singleuser(' '.join(cmd), self.user.name)
        output = output.decode() # convert bytes object to string
        self.log.debug("Stdout of trying to call run_jupyterhub_singleuser(): %s" % output)
        self.job_id = output.split(' ')[-1] # the job id should be the very last part of the string

        # make sure jobid is really a number
        try:
            int(self.job_id)
        except ValueError:
            self.log.info("sbatch returned this at the end of their string: %s" % self.job_id)

        #time.sleep(2)
        job_state = self._check_slurm_job_state()
        for i in range(10):
            self.log.info("job_state is %s" % job_state)
            if 'RUNNING' in job_state:
                break
            elif 'PENDING' in job_state:
                job_state = self._check_slurm_job_state()
                time.sleep(1)
            else:
                self.log.info("Job %s failed to start!" % self.job_id)
                return 1 # is this right? Or should I not return, or return a different thing?
                # NOTE MM - no, start/stop don't return anything, server will poll()

        notebook_ip = get_slurm_job_info(self.job_id)

        self.user.server.ip = notebook_ip
        self.log.info("Notebook server ip is %s" % self.user.server.ip)

    @gen.coroutine
    def _signal(self, sig):
        """simple implementation of signal

        we can use it when we are using setuid (we are root)"""
        return True

    @gen.coroutine
    def stop(self, now=False):
        """stop the subprocess

        if `now`, skip waiting for clean shutdown
        """
        status = yield self.poll()
        self.log.info("*** Stopping notebook for user %s. Status is currently %s ****" % (self.user.name, status))
        if status is not None:
            # job is not running
            return

        cmd = 'scancel ' + self.job_id
        self.log.info("cancelling job %s" % self.job_id)

        job_state = run_command(cmd)
        
        if job_state in ("CANCELLED", "COMPLETED", "FAILED", "COMPLETING"):
            return
        else:
            status = yield self.poll()
            if status is None:
                self.log.warn("Job %s never cancelled" % self.job_id)


if __name__ == "__main__":

        run_jupyterhub_singleuser("jupyterhub-singleuser", 3434)

# vim: set ai expandtab softtabstop=4:
