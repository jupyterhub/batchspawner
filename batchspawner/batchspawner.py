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
import pwd
import os

import xml.etree.ElementTree as ET

from tornado import gen
from tornado.process import Subprocess
from subprocess import CalledProcessError
from tornado.iostream import StreamClosedError

from jupyterhub.spawner import Spawner
from traitlets import (
    Integer, Unicode, Float, Dict, default
)

from jupyterhub.utils import random_port
from jupyterhub.spawner import set_user_setuid
import jupyterhub


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
    start_timeout = Integer(300).tag(config=True)

    # override default server ip since batch jobs normally running remotely
    ip = Unicode("0.0.0.0", help="Address for singleuser server to listen at").tag(config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_queue = Unicode('', \
        help="Queue name to submit job to resource manager"
        ).tag(config=True)

    req_host = Unicode('', \
        help="Host name of batch server to submit job to resource manager"
        ).tag(config=True)

    req_memory = Unicode('', \
        help="Memory to request from resource manager"
        ).tag(config=True)

    req_nprocs = Unicode('', \
        help="Number of processors to request from resource manager"
        ).tag(config=True)

    req_ngpus = Unicode('', \
        help="Number of GPUs to request from resource manager"
        ).tag(config=True)

    req_runtime = Unicode('', \
        help="Length of time for submitted job to run"
        ).tag(config=True)

    req_partition = Unicode('', \
        help="Partition name to submit job to resource manager"
        ).tag(config=True)

    req_account = Unicode('', \
        help="Account name string to pass to the resource manager"
        ).tag(config=True)

    req_options = Unicode('', \
        help="Other options to include into job submission script"
        ).tag(config=True)

    req_username = Unicode()
    @default('req_username')
    def _req_username_default(self):
        return self.user.name

    # Useful IF getpwnam on submit host returns correct info for exec host
    req_homedir = Unicode()
    @default('req_homedir')
    def _req_homedir_default(self):
        return pwd.getpwnam(self.user.name).pw_dir

    req_keepvars = Unicode()
    @default('req_keepvars')
    def _req_keepvars_default(self):
        return ','.join(self.get_env().keys())

    batch_script = Unicode('', \
        help="Template for job submission script. Traits on this class named like req_xyz "
             "will be substituted in the template for {xyz} using string.Formatter. "
             "Must include {cmd} which will be replaced with the jupyterhub-singleuser command line."
        ).tag(config=True)

    # Raw output of job submission command unless overridden
    job_id = Unicode()

    # Will get the raw output of the job status command unless overridden
    job_status = Unicode()

    # Will get the address of the server as reported by job manager
    current_ip = Unicode()

    # Prepare substitution variables for templates using req_xyz traits
    def get_req_subvars(self):
        reqlist = [ t for t in self.trait_names() if t.startswith('req_') ]
        subvars = {}
        for t in reqlist:
            subvars[t[4:]] = getattr(self, t)
        return subvars

    batch_submit_cmd = Unicode('', \
        help="Command to run to submit batch scripts. Formatted using req_xyz traits as {xyz}."
        ).tag(config=True)

    def parse_job_id(self, output):
        "Parse output of submit command to get job id."
        return output

    def cmd_formatted_for_batch(self):
        return ' '.join(self.cmd + self.get_args())

    @gen.coroutine
    def run_command(self, cmd, input=None, env=None):
        proc = Subprocess(cmd, shell=True, env=env, stdin=Subprocess.STREAM, stdout=Subprocess.STREAM,stderr=Subprocess.STREAM)
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
        eout = yield proc.stderr.read_until_close()
        proc.stdout.close()
        proc.stderr.close()
        eout = eout.decode().strip()
        try:
            err = yield proc.wait_for_exit()
        except CalledProcessError:
            self.log.error("Subprocess returned exitcode %s" % proc.returncode)
            self.log.error(eout)
            raise RuntimeError(eout)
        if err != 0:
            return err # exit error?
        else:
            out = out.decode().strip()
            return out

    @gen.coroutine
    def submit_batch_script(self):
        subvars = self.get_req_subvars()
        cmd = self.batch_submit_cmd.format(**subvars)
        subvars['cmd'] = self.cmd_formatted_for_batch()
        if hasattr(self, 'user_options'):
            subvars.update(self.user_options)
        script = self.batch_script.format(**subvars)
        self.log.info('Spawner submitting job using ' + cmd)
        self.log.info('Spawner submitted script:\n' + script)
        out = yield self.run_command(cmd, input=script, env=self.get_env())
        try:
            self.log.info('Job submitted. cmd: ' + cmd + ' output: ' + out)
            self.job_id = self.parse_job_id(out)
        except:
            self.log.error('Job submission failed with exit code ' + out)
            self.job_id = ''
        return self.job_id

    # Override if your batch system needs something more elaborate to read the job status
    batch_query_cmd = Unicode('', \
        help="Command to run to read job status. Formatted using req_xyz traits as {xyz} "
             "and self.job_id as {job_id}."
        ).tag(config=True)

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
            out = yield self.run_command(cmd)
            self.job_status = out
        except Exception as e:
            self.log.error('Error querying job ' + self.job_id)
            self.job_status = ''
        finally:
            return self.job_status

    batch_cancel_cmd = Unicode('',
        help="Command to stop/cancel a previously submitted job. Formatted like batch_query_cmd."
        ).tag(config=True)

    @gen.coroutine
    def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars['job_id'] = self.job_id
        cmd = self.batch_cancel_cmd.format(**subvars)
        self.log.info('Cancelling job ' + self.job_id + ': ' + cmd)
        yield self.run_command(cmd)

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

    startup_poll_interval = Float(0.5, \
        help="Polling interval (seconds) to check job state during startup"
        ).tag(config=True)

    @gen.coroutine
    def start(self):
        """Start the process"""
        if self.user and self.user.server and self.user.server.port:
            self.port = self.user.server.port
            self.db.commit()
        elif (jupyterhub.version_info < (0,7) and not self.user.server.port)  or \
             (jupyterhub.version_info >= (0,7) and not self.port):
            self.port = random_port()
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

        self.current_ip = self.state_gethost()
        if jupyterhub.version_info < (0,7):
            # store on user for pre-jupyterhub-0.7:
            self.user.server.port = self.port
            self.user.server.ip = self.current_ip
        self.db.commit()
        self.log.info("Notebook server job {0} started at {1}:{2}".format(
                        self.job_id, self.current_ip, self.port)
            )

        return self.current_ip, self.port

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
                             self.job_id, self.current_ip, self.port)
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
    state_pending_re = Unicode('',
        help="Regex that matches job_status if job is waiting to run").tag(config=True)
    state_running_re = Unicode('',
        help="Regex that matches job_status if job is running").tag(config=True)
    state_exechost_re = Unicode('',
        help="Regex with at least one capture group that extracts "
             "the execution host from job_status output").tag(config=True)
    state_exechost_exp = Unicode('',
        help="""If empty, notebook IP will be set to the contents of the first capture group.

        If this variable is set, the match object will be expanded using this string
        to obtain the notebook IP.
        See Python docs: re.match.expand""").tag(config=True)

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
""").tag(config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} qsub').tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('sudo -E -u {username} qstat -x {job_id}').tag(config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} qdel {job_id}').tag(config=True)
    # search XML string for job_state - [QH] = pending, R = running, [CE] = done
    state_pending_re = Unicode(r'<job_state>[QH]</job_state>').tag(config=True)
    state_running_re = Unicode(r'<job_state>R</job_state>').tag(config=True)
    state_exechost_re = Unicode(r'<exec_host>((?:[\w_-]+\.?)+)/\d+').tag(config=True)

class MoabSpawner(TorqueSpawner):
    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} msub').tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('sudo -E -u {username} mdiag -j {job_id} --xml').tag(config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} mjobctl -c {job_id}').tag(config=True)
    state_pending_re = Unicode(r'State="Idle"').tag(config=True)
    state_running_re = Unicode(r'State="Running"').tag(config=True)
    state_exechost_re = Unicode(r'AllocNodeList="([^\r\n\t\f :"]*)').tag(config=True)

class UserEnvMixin:
    """Mixin class that computes values for USER, SHELL and HOME in the environment passed to
    the job submission subprocess in case the batch system needs these for the batch script."""

    def user_env(self, env):
        """get user environment"""
        env['USER'] = self.user.name
        home = pwd.getpwnam(self.user.name).pw_dir
        shell = pwd.getpwnam(self.user.name).pw_shell
        if home:
            env['HOME'] = home
        if shell:
            env['SHELL'] = shell
        return env

    def get_env(self):
        """Add user environment variables"""
        env = super().get_env()
        env = self.user_env(env)
        return env

class SlurmSpawner(UserEnvMixin,BatchSpawnerRegexStates):
    """A Spawner that just uses Popen to start local processes."""

    # all these req_foo traits will be available as substvars for templated strings
    req_cluster = Unicode('', \
        help="Cluster name to submit job to resource manager"
        ).tag(config=True)

    req_qos = Unicode('', \
        help="QoS name to submit job to resource manager"
        ).tag(config=True)

    batch_script = Unicode("""#!/bin/bash
#SBATCH --partition={partition}
#SBATCH --time={runtime}
#SBATCH --output={homedir}/jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
#SBATCH --workdir={homedir}
#SBATCH --mem={memory}
#SBATCH --export={keepvars}
#SBATCH --get-user-env=L
#SBATCH {options}

which jupyterhub-singleuser
{cmd}
""").tag(config=True)
    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode('sudo -E -u {username} sbatch --parsable').tag(config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode("sudo -E -u {username} squeue -h -j {job_id} -o '%T %B'").tag(config=True) #
    batch_cancel_cmd = Unicode('sudo -E -u {username} scancel {job_id}').tag(config=True)
    # use long-form states: PENDING,  CONFIGURING = pending
    #  RUNNING,  COMPLETING = running
    state_pending_re = Unicode(r'^(?:PENDING|CONFIGURING)').tag(config=True)
    state_running_re = Unicode(r'^(?:RUNNING|COMPLETING)').tag(config=True)
    state_exechost_re = Unicode(r'\s+((?:[\w_-]+\.?)+)$').tag(config=True)

    def parse_job_id(self, output):
        # make sure jobid is really a number
        try:
            id = output.split(';')[0]
            int(id)
        except Exception as e:
            self.log.error("SlurmSpawner unable to parse job ID from text: " + output)
            raise e
        return id

class MultiSlurmSpawner(SlurmSpawner):
    '''When slurm has been compiled with --enable-multiple-slurmd, the
       administrator sets the name of the slurmd instance via the slurmd -N
       option. This node name is usually different from the hostname and may
       not be resolvable by JupyterHub. Here we enable the administrator to
       map the node names onto the real hostnames via a traitlet.'''
    daemon_resolver = Dict({}, help="Map node names to hostnames").tag(config=True)

    def state_gethost(self):
        host = SlurmSpawner.state_gethost(self)
        return self.daemon_resolver.get(host, host)

class GridengineSpawner(BatchSpawnerBase):
    batch_script = Unicode("""#!/bin/bash
#$ -j yes
#$ -N spawner-jupyterhub
#$ -o {homedir}/.jupyterhub.sge.out
#$ -e {homedir}/.jupyterhub.sge.err
#$ -v {keepvars}
#$ {options}

{cmd}
""").tag(config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} qsub').tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('sudo -E -u {username} qstat -xml').tag(config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} qdel {job_id}').tag(config=True)

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

class CondorSpawner(UserEnvMixin,BatchSpawnerRegexStates):
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
""").tag(config=True)

    # outputs job id string
    batch_submit_cmd = Unicode('sudo -E -u {username} condor_submit').tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode('condor_q {job_id} -format "%s, " JobStatus -format "%s" RemoteHost -format "\n" True').tag(config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} condor_rm {job_id}').tag(config=True)
    # job status: 1 = pending, 2 = running
    state_pending_re = Unicode(r'^1,').tag(config=True)
    state_running_re = Unicode(r'^2,').tag(config=True)
    state_exechost_re = Unicode(r'^\w*, .*@([^ ]*)').tag(config=True)

    def parse_job_id(self, output):
        match = re.search(r'.*submitted to cluster ([0-9]+)', output)
        if match:
            return match.groups()[0]

        error_msg = "CondorSpawner unable to parse jobID from text: " + output
        self.log.error(error_msg)
        raise Exception(error_msg)

    def cmd_formatted_for_batch(self):
        return super(CondorSpawner,self).cmd_formatted_for_batch().replace('"','""').replace("'","''")

class LsfSpawner(BatchSpawnerBase):
    '''A Spawner that uses IBM's Platform Load Sharing Facility (LSF) to launch notebooks.'''

    batch_script = Unicode('''#!/bin/sh
    #BSUB -R "select[type==any]"    # Allow spawning on non-uniform hardware
    #BSUB -R "span[hosts=1]"        # Only spawn job on one server
    #BSUB -q {queue}
    #BSUB -J spawner-jupyterhub
    #BSUB -o {homedir}/.jupyterhub.lsf.out
    #BSUB -e {homedir}/.jupyterhub.lsf.err

    {cmd}
    ''').tag(config=True)


    batch_submit_cmd = Unicode('sudo -E -u {username} bsub').tag(config=True)
    batch_query_cmd = Unicode('sudo -E -u {username} bjobs -a -noheader -o "STAT EXEC_HOST" {job_id}').tag(config=True)
    batch_cancel_cmd = Unicode('sudo -E -u {username} bkill {job_id}').tag(config=True)

    def get_env(self):
        env = super().get_env()

        # LSF relies on environment variables to launch local jobs.  Ensure that these values are included
        # in the environment used to run the spawner.
        for key in ['LSF_ENVDIR','LSF_SERVERDIR','LSF_FULL_VERSION','LSF_LIBDIR','LSF_BINDIR']:
            if key in os.environ and key not in env:
                env[key] = os.environ[key]
        return env

    def parse_job_id(self, output):
        # Assumes output in the following form:
        # "Job <1815> is submitted to default queue <normal>."
        return output.split(' ')[1].strip('<>')

    def state_ispending(self):
        # Parse results of batch_query_cmd
        # Output determined by results of self.batch_query_cmd
        if self.job_status:
            return self.job_status.split(' ')[0].upper() in {'PEND', 'PUSP'}

    def state_isrunning(self):
        if self.job_status:
            return self.job_status.split(' ')[0].upper() == 'RUN'


    def state_gethost(self):
        if self.job_status:
            return self.job_status.split(' ')[1].strip()

        self.log.error("Spawner unable to match host addr in job {0} with status {1}".format(self.job_id, self.job_status))
        return

# vim: set ai expandtab softtabstop=4:
