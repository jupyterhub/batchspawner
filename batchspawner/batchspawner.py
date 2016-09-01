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
import pwd
import os
from subprocess import Popen, call
import subprocess

import xml.etree.ElementTree as ET

from tornado import gen
from tornado.process import Subprocess
from tornado.iostream import StreamClosedError

from jupyterhub.spawner import Spawner
from traitlets import (
    Instance, Integer, Unicode, Float
)

from jupyterhub.utils import random_port
from jupyterhub.spawner import set_user_setuid

@gen.coroutine
def run_command(cmd, input=None, env=None):
    proc = Subprocess(cmd, shell=True, env=env, stdin=Subprocess.STREAM, stdout=Subprocess.STREAM)
    inbytes = None
    if input:
        inbytes = input.encode()
        try:
            yield proc.stdin.write(inbytes)
        except StreamClosedError as exp:
            # Apparently harmless
            pass
    proc.stdin.close()
    out = yield proc.stdout.read_until_close()
    proc.stdout.close()
    err = yield proc.wait_for_exit()
    if err != 0:
        return err # exit error?
    else:
        out = out.decode().strip()
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

    req_options = Unicode('', config=True, \
        help="Other options to include into job submission script"
        )

    req_username = Unicode()
    def _req_username_default(self):
        return self.user.name

    # Useful IF getpwnam on submit host returns correct info for exec host
    req_homedir = Unicode()
    def _req_homedir_default(self):
        return pwd.getpwnam(self.user.name).pw_dir

    req_keepvars = Unicode()
    def _req_keepvars_default(self):
        return ','.join(self.get_env().keys())

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

    def parse_job_id(self, output):
        "Parse output of submit command to get job id."
        return output

    @gen.coroutine
    def submit_batch_script(self):
        subvars = self.get_req_subvars()
        cmd = self.batch_submit_cmd.format(**subvars)
        subvars['cmd'] = ' '.join(self.cmd + self.get_args())
        if hasattr(self, 'user_options'):
            subvars['user_options'] = self.user_options
        script = self.batch_script.format(**subvars)
        self.log.info('Spawner submitting job using ' + cmd)
        self.log.info('Spawner submitted script:\n' + script)
        out = yield run_command(cmd, input=script, env=self.get_env())
        try:
            self.log.info('Job submitted. cmd: ' + cmd + ' output: ' + out)
            self.job_id = self.parse_job_id(out)
        except:
            self.log.error('Job submission failed with exit code ' + out)
            self.job_id = ''
        return self.job_id

    # Override if your batch system needs something more elaborate to read the job status
    batch_query_cmd = Unicode('', config=True, \
        help="Command to run to read job status. Formatted using req_xyz traits as {xyz} "
             "and self.job_id as {job_id}."
        )

    @gen.coroutine
    def read_job_state(self):
        if self.job_id is None or len(self.job_id) == 0:
            # job not running
            self.job_status = ''
            return self.job_status
        subvars = self.get_req_subvars()
        subvars['job_id'] = self.job_id
        cmd = self.batch_query_cmd.format(**subvars)
        self.log.debug('Spawner querying job: ' + cmd)
        try:
            out = yield run_command(cmd)
            self.job_status = out
        except Exception as e:
            self.log.error('Error querying job ' + self.job_id)
            self.job_status = ''
        finally:
            return self.job_status

    batch_cancel_cmd = Unicode('', config=True,
        help="Command to stop/cancel a previously submitted job. Formatted like batch_query_cmd."
        )

    @gen.coroutine
    def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars['job_id'] = self.job_id
        cmd = self.batch_cancel_cmd.format(**subvars)
        self.log.info('Cancelling job ' + self.job_id + ': ' + cmd)
        yield run_command(cmd)

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
            yield self.read_job_state()
            if self.state_isrunning() or self.state_ispending():
                return None
            else:
                self.clear_state()
                return 1

        if not self.job_id:
            # no job id means it's not running
            self.clear_state()
            return 1

    startup_poll_interval = Float(0.5, config=True, \
        help="Polling interval (seconds) to check job state during startup"
        )

    @gen.coroutine
    def start(self):
        """Start the process"""
        if not self.user.server.port:
            self.user.server.port = random_port()
            self.db.commit()
        job = yield self.submit_batch_script()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called. 
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        assert len(self.job_id) > 0
        while True:
            yield self.poll()
            if self.state_isrunning():
                break
            else:
                if self.state_ispending():
                    self.log.debug('Job ' + self.job_id + ' still pending')
                else:
                    self.log.warn('Job ' + self.job_id + ' neither pending nor running.\n' +
                        self.job_status)
                assert self.state_ispending()
            yield gen.sleep(self.startup_poll_interval)

        self.user.server.ip = self.state_gethost()
        self.db.commit()
        self.log.info("Notebook server job {0} started at {1}:{2}".format(
                        self.job_id, self.user.server.ip, self.user.server.port)
            )

    @gen.coroutine
    def stop(self, now=False):
        """Stop the singleuser server job.

        Returns immediately after sending job cancellation command if now=True, otherwise
        tries to confirm that job is no longer running."""

        self.log.info("Stopping server job " + self.job_id)
        yield self.cancel_batch_job()
        if now:
            return
        for i in range(10):
            yield self.poll()
            if not self.state_isrunning():
                return
            yield gen.sleep(1.0)
        if self.job_id:
            self.log.warn("Notebook server job {0} at {1}:{2} possibly failed to terminate".format(
                             self.job_id, self.user.server.ip, self.user.server.port)
                )

import re

class BatchSpawnerRegexStates(BatchSpawnerBase):
    """Subclass of BatchSpawnerBase that uses config-supplied regular expressions
    to interact with batch submission system state. Provides implementations of
        state_ispending
        state_isrunning
        state_gethost

    In their place, the user should supply the following configuration:
        state_pending_re - regex that matches job_status if job is waiting to run
        state_running_re - regex that matches job_status if job is running
        state_exechost_re - regex with at least one capture group that extracts
                            execution host from job_status
        state_exechost_exp - if empty, notebook IP will be set to the contents of the
            first capture group. If this variable is set, the match object
            will be expanded using this string to obtain the notebook IP.
            See Python docs: re.match.expand
    """
    state_pending_re = Unicode('', config=True,
        help="Regex that matches job_status if job is waiting to run")
    state_running_re = Unicode('', config=True,
        help="Regex that matches job_status if job is running")
    state_exechost_re = Unicode('', config=True,
        help="Regex with at least one capture group that extracts "
             "the execution host from job_status output")
    state_exechost_exp = Unicode('', config=True,
        help="""If empty, notebook IP will be set to the contents of the first capture group.

        If this variable is set, the match object will be expanded using this string
        to obtain the notebook IP.
        See Python docs: re.match.expand""")

    def state_ispending(self):
        assert self.state_pending_re
        if self.job_status and re.search(self.state_pending_re, self.job_status):
            return True
        else: return False

    def state_isrunning(self):
        assert self.state_running_re
        if self.job_status and re.search(self.state_running_re, self.job_status):
            return True
        else: return False

    def state_gethost(self):
        assert self.state_exechost_re
        match = re.search(self.state_exechost_re, self.job_status)
        if not match:
            self.log.error("Spawner unable to match host addr in job status: " + self.job_status)
            return
        if not self.state_exechost_exp:
            return match.groups()[0]
        else:
            return match.expand(self.state_exechost_exp)

class TorqueSpawner(BatchSpawnerRegexStates):
    batch_script = Unicode("""#!/bin/sh
#PBS -q {queue}@{host}
#PBS -l walltime={runtime}
#PBS -l nodes=1:ppn={nprocs}
#PBS -l mem={memory}
#PBS -N jupyterhub-singleuser
#PBS -v {keepvars}
#PBS {options}

{cmd}
""",
        config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} qsub', config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('sudo -E -u {username} qstat -x {job_id}', config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} qdel {job_id}', config=True)
    # search XML string for job_state - [QH] = pending, R = running, [CE] = done
    state_pending_re = Unicode(r'<job_state>[QH]</job_state>', config=True)
    state_running_re = Unicode(r'<job_state>R</job_state>', config=True)
    state_exechost_re = Unicode(r'<exec_host>((?:[\w_-]+\.?)+)/\d+', config=True)

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

class SlurmSpawner(BatchSpawnerRegexStates,UserEnvMixin):
    """A Spawner that just uses Popen to start local processes."""

    # all these req_foo traits will be available as substvars for templated strings
    req_partition = Unicode('', config=True, \
        help="Partition name to submit job to resource manager"
        )

    req_qos = Unicode('', config=True, \
        help="QoS name to submit job to resource manager"
        )

    batch_script = Unicode("""#!/bin/bash
#SBATCH --partition={partition}
#SBATCH --time={runtime}
#SBATCH --output={homedir}/jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
#SBATCH --workdir={homedir}
#SBATCH --mem={memory}
#SBATCH --export={keepvars}
#SBATCH --uid={username}
#SBATCH --get-user-env=L
#SBATCH {options}

which jupyterhub-singleuser
{cmd}
""",
        config=True)
    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode('sudo -E -u {username} sbatch', config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode('sudo -E -u {username} squeue -h -j {job_id} -o "%T %B"', config=True) #
    batch_cancel_cmd = Unicode('sudo -E -u {username} scancel {job_id}', config=True)
    # use long-form states: PENDING,  CONFIGURING = pending
    #  RUNNING,  COMPLETING = running
    state_pending_re = Unicode(r'^(?:PENDING|CONFIGURING)', config=True)
    state_running_re = Unicode(r'^(?:RUNNING|COMPLETING)', config=True)
    state_exechost_re = Unicode(r'\s+((?:[\w_-]+\.?)+)$', config=True)

    def parse_job_id(self, output):
        # make sure jobid is really a number
        try:
            id = output.split(' ')[-1]
            int(id)
        except Exception as e:
            self.log.error("SlurmSpawner unable to parse job ID from text: " + output)
            raise e
        return id

class GridengineSpawner(BatchSpawnerBase):
    batch_script = Unicode("""#!/bin/bash
#$ -j yes
#$ -N spawner-jupyterhub
#$ -v {keepvars}
#$ {options}

{cmd}
""",
        config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} qsub', config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('sudo -E -u {username} qstat -xml', config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} qdel {job_id}', config=True)

    def parse_job_id(self, output):
        return output.split(' ')[2]

    def state_ispending(self):
        if self.job_status:
            job_info = ET.fromstring(self.job_status).find(
                    ".//job_list[JB_job_number='{0}']".format(self.job_id))
            if job_info is not None:
                return job_info.attrib.get('state') == 'pending'
        return False

    def state_isrunning(self):
        if self.job_status:
            job_info = ET.fromstring(self.job_status).find(
                    ".//job_list[JB_job_number='{0}']".format(self.job_id))
            if job_info is not None:
                return job_info.attrib.get('state') == 'running'
        return False

    def state_gethost(self):
        if self.job_status:
            queue_name = ET.fromstring(self.job_status).find(
                    ".//job_list[JB_job_number='{0}']/queue_name".format(self.job_id))
            if queue_name is not None and queue_name.text:
                return queue_name.text.split('@')[1]

        self.log.error("Spawner unable to match host addr in job {0} with status {1}".format(self.job_id, self.job_status))
        return

class CondorSpawner(BatchSpawnerRegexStates):
    batch_script = Unicode("""
Executable = /bin/sh
RequestMemory = {memory}
RequestCpus = {nprocs}
Arguments = \"-c 'exec {cmd}'\"
Remote_Initialdir = {homedir}
Output = {homedir}/.jupyterhub.condor.out
Error = {homedir}/.jupyterhub.condor.err
ShouldTransferFiles = False
GetEnv = True
{options}
Queue
""",
        config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} condor_submit', config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('condor_q {job_id} -format "%s, " JobStatus -format "%s" RemoteHost -format "\n" True', config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} condor_rm {job_id}', config=True)
    # job status: 1 = pending, 2 = running
    state_pending_re = Unicode(r'^1,', config=True)
    state_running_re = Unicode(r'^2,', config=True)
    state_exechost_re = Unicode(r'^\w*, .*@([^ ]*)', config=True)

    def parse_job_id(self, output):
        match = re.search(r'.*submitted to cluster ([0-9]+)', output)
        if match:
            return match.groups()[0]

        error_msg = "CondorSpawner unable to parse jobID from text: " + output
        self.log.error(error_msg)
        raise Exception(error_msg)

# vim: set ai expandtab softtabstop=4:
