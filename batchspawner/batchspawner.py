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

def run_command(cmd):
    popen = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    out = popen.communicate()
    if out[1] is not None:
        return out[1] # exit error?
    else:
        out = out[0].decode().strip()
        return out

def get_slurm_job_info(jobid):
    """returns ip address of node that is running the job"""
    cmd = 'squeue -h -j ' + jobid + ' -o %N'
    print(cmd)
    popen = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    node_name = popen.communicate()[0].strip().decode() # convett bytes object to string
    # now get the ip address of the node name
    cmd = 'host %s' % node_name
    print(cmd)
    popen = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    out = popen.communicate()[0].strip().decode()
    node_ip = out.split(' ')[-1] # the last portion of the output should be the ip address

    return node_ip

def run_jupyterhub_singleuser(cmd, user):
    sbatch = Template('''#!/bin/bash
#SBATCH --partition=$queue
#SBATCH --time=$hours:00:00
#SBATCH -o /home/$user/jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
#SBATCH --workdir=/home/$user
#SBATCH --mem=$mem
###SBATCH --export=ALL
#SBATCH --uid=$user
#SBATCH --get-user-env=L

which jupyterhub-singleuser
$export_cmd
$cmd
    ''')

    queue = "all"
    mem = '200'
    hours = '2'
    full_cmd = cmd.split(';')
    export_cmd = full_cmd[0]
    cmd = full_cmd[1]
    sbatch = sbatch.substitute(dict(export_cmd=export_cmd, cmd=cmd, queue=queue, mem=mem, hours=hours, user=user))
    #serialsbatch+='cd %s' % "notebooks"
    print('Submitting *****{\n%s\n}*****' % sbatch)
    popen = subprocess.Popen('sbatch', shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE)
    out = popen.communicate(sbatch.encode())[0].strip() #e.g. something like "Submitted batch job 209"
    return out

class BatchSpawnerBase(Spawner):
    """Base class for spawners using resource manager batch job submission mechanisms"""

    INTERRUPT_TIMEOUT = Integer(1200, config=True, \
        help="Seconds to wait for process to halt after SIGINT before proceeding to SIGTERM"
                               )
    TERM_TIMEOUT = Integer(1200, config=True, \
        help="Seconds to wait for process to halt after SIGTERM before proceeding to SIGKILL"
                          )
    KILL_TIMEOUT = Integer(1200, config=True, \
        help="Seconds to wait for process to halt after SIGKILL before giving up"
                          )
    batch_queue = Unicode('', config=True, \
        help="Queue name to submit job to resource manager"
        )

    batch_host = Unicode('', config=True, \
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

    batch_script = Unicode('', config=True, \
        help="Template for job submission script. Traits on this class named like req_xyz "
             "will be substituted in the template for {xyz} using string.Formatter"
        )

    job_id = Unicode()

    def format_req_script(self):
        reqlist = [ t for t in self.trait_names() if t.startswith('req_') ]
        subvars = {}
        for t in reqlist:
            subvars[t[4:]] = getattr(self, t)
        return self.batch_script.format(**subvars)

    def load_state(self, state):
        """load job_id from state"""
        super(BatchSpawnerBase, self).load_state(state)
        self.job_id = state.get('job_id', '')

    def get_state(self):
        """add job_id to state"""
        state = super(BatchSpawnerBase, self).get_state()
        if self.job_id:
            state['job_id'] = self.job_id
        return state

    def clear_state(self):
        """clear job_id state"""
        super(BatchSpawnerBase, self).clear_state()
        self.job_id = ""


class SlurmSpawner(BatchSpawnerBase):
    """A Spawner that just uses Popen to start local processes."""

    ip = Unicode("0.0.0.0", config=True, \
        help="url of the server")

    def make_preexec_fn(self, name):
        """make preexec fn"""
        return set_user_setuid(name)

    def user_env(self, env):
        """get user environment"""
        env['USER'] = self.user.name
        env['HOME'] = pwd.getpwnam(self.user.name).pw_dir
        return env

    def _env_default(self):
        env = super()._env_default()
        return self.user_env(env)

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
        
        notebook_ip = get_slurm_job_info(self.job_id)

        self.user.server.ip = notebook_ip 
        self.log.info("Notebook server ip is %s" % self.user.server.ip)

    @gen.coroutine
    def poll(self):
        """Poll the process"""
        if self.job_id is not None:
            state = self._check_slurm_job_state()
            if "RUNNING" in state or "PENDING" in state:
                return None
            else:
                self.clear_state()
                return 1

        if not self.job_id:
            # no job id means it's not running
            self.clear_state()
            return 1

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
