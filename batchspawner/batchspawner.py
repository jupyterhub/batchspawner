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
import asyncio
import random
import subprocess
import tempfile
from textwrap import dedent
from async_generator import async_generator, yield_
import asyncio, asyncssh, sys
import pwd
import time
import os
import re

import xml.etree.ElementTree as ET

from enum import Enum

from jinja2 import Template

from tornado import gen

from jupyterhub.spawner import Spawner
from traitlets import Integer, Unicode, Float, Dict, default

from jupyterhub.spawner import set_user_setuid


def format_template(template, *args, **kwargs):
    """Format a template, either using jinja2 or str.format().

    Use jinja2 if the template is a jinja2.Template, or contains '{{' or
    '{%'.  Otherwise, use str.format() for backwards compatability with
    old scripts (but you can't mix them).
    """
    if isinstance(template, Template):
        return template.render(*args, **kwargs)
    elif "{{" in template or "{%" in template:
        return Template(template).render(*args, **kwargs)
    return template.format(*args, **kwargs)


class JobStatus(Enum):
    NOTFOUND = 0
    RUNNING = 1
    PENDING = 2
    UNKNOWN = 3


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
    start_timeout = Integer(3600).tag(config=True)

    # override default server ip since batch jobs normally running remotely
    ip = Unicode(
        "0.0.0.0",
        help="Address for singleuser server to listen at",
    ).tag(config=True)

    exec_prefix = Unicode(
        "",
        # "sudo -E -u {username}",
        help="Standard execution prefix (e.g. the default sudo -E -u {username})",
    ).tag(config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_queue = Unicode(
        "",
        help="Queue name to submit job to resource manager",
    ).tag(config=True)

    req_host = Unicode(
        "",
        help="Host name of batch server to submit job to resource manager",
    ).tag(config=True)

    req_memory = Unicode(
        "",
        help="Memory to request from resource manager",
    ).tag(config=True)

    req_nprocs = Unicode(
        "8",
        help="Number of processors to request from resource manager",
    ).tag(config=True)

    req_ngpus = Unicode(
        "",
        help="Number of GPUs to request from resource manager",
    ).tag(config=True)

    req_runtime = Unicode(
        "",
        help="Length of time for submitted job to run",
    ).tag(config=True)

    req_partition = Unicode(
        "",
        help="Partition name to submit job to resource manager",
    ).tag(config=True)

    req_account = Unicode(
        "",
        help="Account name string to pass to the resource manager",
    ).tag(config=True)

    req_options = Unicode(
        "",
        help="Other options to include into job submission script",
    ).tag(config=True)

    req_prologue = Unicode(
        "",
        help="Script to run before single user server starts.",
    ).tag(config=True)

    req_epilogue = Unicode(
        "",
        help="Script to run after single user server ends.",
    ).tag(config=True)

    req_username = Unicode()

    @default("req_username")
    def _req_username_default(self):
        return self.user.name

    # Useful IF getpwnam on submit host returns correct info for exec host
    req_homedir = Unicode()

    @default("req_homedir")
    def _req_homedir_default(self):
        return pwd.getpwnam(self.user.name).pw_dir

    req_keepvars = Unicode()

    @default("req_keepvars")
    def _req_keepvars_default(self):
        return ",".join(self.get_env().keys())

    req_keepvars_extra = Unicode(
        help="Extra environment variables which should be configured, "
        "added to the defaults in keepvars, "
        "comma separated list.",
    )

    batch_script = Unicode(
        "",
        help="Template for job submission script. Traits on this class named like req_xyz "
        "will be substituted in the template for {xyz} using string.Formatter. "
        "Must include {cmd} which will be replaced with the jupyterhub-singleuser command line.",
    ).tag(config=True)

    batchspawner_singleuser_cmd = Unicode(
        "batchspawner-singleuser",
        help="A wrapper which is capable of special batchspawner setup: currently sets the port on "
        "the remote host.  Not needed to be set under normal circumstances, unless path needs "
        "specification.",
    ).tag(config=True)

    # Raw output of job submission command unless overridden
    job_id = Unicode()

    # Will get the raw output of the job status command unless overridden
    job_status = Unicode()

    # Prepare substitution variables for templates using req_xyz traits
    def get_req_subvars(self):
        reqlist = [t for t in self.trait_names() if t.startswith("req_")]
        subvars = {}
        for t in reqlist:
            subvars[t[4:]] = getattr(self, t)
        if subvars.get("keepvars_extra"):
            subvars["keepvars"] += "," + subvars["keepvars_extra"]
        return subvars

    batch_submit_cmd = Unicode(
        "",
        help="Command to run to submit batch scripts. Formatted using req_xyz traits as {xyz}.",
    ).tag(config=True)

    def parse_job_id(self, output):
        "Parse output of submit command to get job id."
        return output

    def cmd_formatted_for_batch(self):
        """The command which is substituted inside of the batch script"""
        return " ".join([self.batchspawner_singleuser_cmd] + self.cmd + self.get_args())

    async def run_command(self, cmd, input=None, env=None):
        if getattr(self, "input_as_file", False) and input is not None:
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                f.write(input)
                f.flush()
                cmd = cmd + " " + f.name
                input = None

        # self.log.info("Running command: %s" % cmd)
        # out = subprocess.check_output(cmd, shell=True, env=env).decode()

        # return out

        proc = await asyncio.create_subprocess_shell(
            cmd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        inbytes = None

        if input:
            inbytes = input.encode()

        try:
            out, eout = await proc.communicate(input=inbytes)
        except:
            self.log.debug("Exception raised when trying to run command: %s" % cmd)
            proc.kill()
            self.log.debug("Running command failed, killed process.")
            try:
                out, eout = await asyncio.wait_for(proc.communicate(), timeout=10)
                out = out.decode().strip()
                eout = eout.decode().strip()
                self.log.error("Subprocess returned exitcode %s" % proc.returncode)
                self.log.error("Stdout:")
                self.log.error(out)
                self.log.error("Stderr:")
                self.log.error(eout)
                raise RuntimeError(
                    "{} exit status {}: {}".format(cmd, proc.returncode, eout)
                )
            except asyncio.TimeoutError:
                self.log.error(
                    "Encountered timeout trying to clean up command, process probably killed already: %s"
                    % cmd
                )
                return ""
            except:
                self.log.error(
                    "Encountered exception trying to clean up command: %s" % cmd
                )
                raise
        else:
            eout = eout.decode().strip()
            err = proc.returncode
            if err != 0:
                self.log.error("Subprocess returned exitcode %s" % err)
                self.log.error(eout)
                raise RuntimeError(eout)

        out = out.decode().strip()
        return out

    async def _get_batch_script(self, **subvars):
        """Format batch script from vars"""
        # Could be overridden by subclasses, but mainly useful for testing
        return format_template(self.batch_script, **subvars)

    async def submit_batch_script(self):
        subvars = self.get_req_subvars()
        # `cmd` is submitted to the batch system
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_submit_cmd, **subvars),
            )
        )
        # `subvars['cmd']` is what is run _inside_ the batch script,
        # put into the template.
        subvars["cmd"] = self.cmd_formatted_for_batch()
        if hasattr(self, "user_options"):
            subvars.update(self.user_options)
        script = await self._get_batch_script(**subvars)
        self.log.info("Spawner submitting job using " + cmd)
        self.log.info("Spawner submitted script:\n" + script)
        out = await self.run_command(cmd, input=script, env=self.get_env())
        try:
            self.log.info("Job submitted. cmd: " + cmd + " output: " + out)
            self.job_id = self.parse_job_id(out)
        except:
            self.log.error("Job submission failed with exit code " + out)
            self.job_id = ""
        return self.job_id

    # Override if your batch system needs something more elaborate to query the job status
    batch_query_cmd = Unicode(
        "",
        help="Command to run to query job status. Formatted using req_xyz traits as {xyz} "
        "and self.job_id as {job_id}.",
    ).tag(config=True)

    async def query_job_status(self):
        """Check job status, return JobStatus object."""
        if self.job_id is None or len(self.job_id) == 0:
            self.job_status = ""
            return JobStatus.NOTFOUND
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_query_cmd, **subvars),
            )
        )
        self.log.debug("Spawner querying job: " + cmd)
        try:
            self.job_status = await self.run_command(cmd)
        except RuntimeError as e:
            # e.args[0] is stderr from the process
            self.job_status = e.args[0]
        except Exception as e:
            self.log.error("Error querying job " + self.job_id)
            self.job_status = ""

        if self.state_isrunning():
            return JobStatus.RUNNING
        elif self.state_ispending():
            return JobStatus.PENDING
        elif self.state_isunknown():
            return JobStatus.UNKNOWN
        else:
            return JobStatus.NOTFOUND

    batch_cancel_cmd = Unicode(
        "",
        help="Command to stop/cancel a previously submitted job. Formatted like batch_query_cmd.",
    ).tag(config=True)

    async def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_cancel_cmd, **subvars),
            )
        )
        self.log.info("Cancelling job " + self.job_id + ": " + cmd)
        await self.run_command(cmd)

    def load_state(self, state):
        """load job_id from state"""
        super(BatchSpawnerBase, self).load_state(state)
        self.job_id = state.get("job_id", "")
        self.job_status = state.get("job_status", "")

    def get_state(self):
        """add job_id to state"""
        state = super(BatchSpawnerBase, self).get_state()
        if self.job_id:
            state["job_id"] = self.job_id
        if self.job_status:
            state["job_status"] = self.job_status
        return state

    def clear_state(self):
        """clear job_id state"""
        super(BatchSpawnerBase, self).clear_state()
        self.job_id = ""
        self.job_status = ""

    def make_preexec_fn(self, name):
        """make preexec fn to change uid (if running as root) before job submission"""
        return set_user_setuid(name)

    def state_ispending(self):
        "Return boolean indicating if job is still waiting to run, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    def state_isrunning(self):
        "Return boolean indicating if job is running, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    def state_isunknown(self):
        "Return boolean indicating if job state retrieval failed because of the resource manager"
        return None

    def state_gethost(self):
        "Return string, hostname or addr of running job, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    async def poll(self):
        """Poll the process"""
        status = await self.query_job_status()
        if status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.UNKNOWN):
            return None
        else:
            self.clear_state()
            return 1

    startup_poll_interval = Float(
        2,
        help="Polling interval (seconds) to check job state during startup",
    ).tag(config=True)

    async def start(self):
        """Start the process"""
        self.ip = self.traits()["ip"].default_value
        self.port = self.traits()["port"].default_value

        if self.server:
            self.server.port = self.port

        job = await self.submit_batch_script()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter batch job submission failure (no jobid in output)"
            )
        while True:
            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                break
            elif status == JobStatus.PENDING:
                self.log.debug("Job " + self.job_id + " still pending")
            elif status == JobStatus.UNKNOWN:
                self.log.debug("Job " + self.job_id + " still unknown")
            else:
                self.log.warning(
                    "Job "
                    + self.job_id
                    + " neither pending nor running.\n"
                    + self.job_status
                )
                self.clear_state()
                raise RuntimeError(
                    "The Jupyter batch job has disappeared"
                    " while pending in the queue or died immediately"
                    " after starting."
                )
            await gen.sleep(self.startup_poll_interval)

        while True:
            await self.query_job_log()

            try:
                self.state_gethost()
                if self.port != self.traits()["port"].default_value:
                    self.log.info(f"found ip {self.ip} and port {self.port} in log")
                    break
            except Exception as e:
                self.log.warn(f"failed to get ip: {e}")
            else:
                self.log.info("no ip in log yet")

            await gen.sleep(self.startup_poll_interval)

        self.ip = self.state_gethost()
        while self.port == 0:
            await gen.sleep(self.startup_poll_interval)
            # Test framework: For testing, mock_port is set because we
            # don't actually run the single-user server yet.
            if hasattr(self, "mock_port"):
                self.port = self.mock_port

        self.db.commit()
        self.log.info(
            "Notebook server job {0} started at {1}:{2}".format(
                self.job_id, self.ip, self.port
            )
        )

        # TODO: make ssh connection to gw https://github.com/NERSC/sshspawner/blob/master/sshspawner/sshspawner.py

        return self.ip, self.port

    async def stop(self, now=False):
        """Stop the singleuser server job.

        Returns immediately after sending job cancellation command if now=True, otherwise
        tries to confirm that job is no longer running."""

        self.log.info("Stopping server job " + self.job_id)
        await self.cancel_batch_job()
        if now:
            return
        for i in range(10):
            status = await self.query_job_status()
            if status not in (JobStatus.RUNNING, JobStatus.UNKNOWN):
                return
            await gen.sleep(1.0)
        if self.job_id:
            self.log.warning(
                "Notebook server job {0} at {1}:{2} possibly failed to terminate".format(
                    self.job_id, self.ip, self.port
                )
            )

    @async_generator
    async def progress(self):
        while True:
            if self.state_ispending():
                await yield_(
                    {
                        "message": "Pending in queue...",
                    }
                )
            elif self.state_isrunning():
                await yield_(
                    {
                        "message": "Cluster job running... waiting to connect",
                    }
                )
                return
            else:
                await yield_(
                    {
                        "message": "Unknown status...",
                    }
                )
            await gen.sleep(1)


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

    state_pending_re = Unicode(
        "",
        help="Regex that matches job_status if job is waiting to run",
    ).tag(config=True)
    state_running_re = Unicode(
        "",
        help="Regex that matches job_status if job is running",
    ).tag(config=True)
    state_exechost_re = Unicode(
        "",
        help="Regex with at least one capture group that extracts "
        "the execution host from job_status output",
    ).tag(config=True)
    state_exechost_exp = Unicode(
        "",
        help="""If empty, notebook IP will be set to the contents of the first capture group.

        If this variable is set, the match object will be expanded using this string
        to obtain the notebook IP.
        See Python docs: re.match.expand""",
    ).tag(config=True)
    state_unknown_re = Unicode(
        "",
        help="Regex that matches job_status if the resource manager is not answering."
        "Blank indicates not used.",
    ).tag(config=True)

    def state_ispending(self):
        assert self.state_pending_re, "Misconfigured: define state_running_re"
        return self.job_status and re.search(self.state_pending_re, self.job_status)

    def state_isrunning(self):
        assert self.state_running_re, "Misconfigured: define state_running_re"
        return self.job_status and re.search(self.state_running_re, self.job_status)

    def state_isunknown(self):
        # Blank means "not set" and this function always returns None.
        if self.state_unknown_re:
            return self.job_status and re.search(self.state_unknown_re, self.job_status)

    def state_gethost(self):
        assert self.state_exechost_re, "Misconfigured: define state_exechost_re"
        match = re.search(self.state_exechost_re, self.job_status)
        if not match:
            self.log.error(
                "Spawner unable to match host addr in job status: " + self.job_status
            )
            return
        if not self.state_exechost_exp:
            return match.groups()[0]
        else:
            return match.expand(self.state_exechost_exp)


class TorqueSpawner(BatchSpawnerRegexStates):
    batch_script = Unicode(
        """#!/bin/sh
#PBS -q {queue}@{host}
#PBS -l walltime={runtime}
#PBS -l nodes=1:ppn={nprocs}
#PBS -l mem={memory}
#PBS -N jupyterhub-singleuser
#PBS -v {keepvars}
#PBS {options}

set -eu

{prologue}
{cmd}
{epilogue}
"""
    ).tag(config=True)

    # outputs job id string
    batch_submit_cmd = Unicode("qsub").tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode("qstat -x {job_id}").tag(config=True)
    batch_cancel_cmd = Unicode("qdel {job_id}").tag(config=True)
    # search XML string for job_state - [QH] = pending, R = running, [CE] = done
    state_pending_re = Unicode(r"<job_state>[QH]</job_state>").tag(config=True)
    state_running_re = Unicode(r"<job_state>R</job_state>").tag(config=True)
    state_exechost_re = Unicode(r"<exec_host>((?:[\w_-]+\.?)+)/\d+").tag(config=True)


class MoabSpawner(TorqueSpawner):
    # outputs job id string
    batch_submit_cmd = Unicode("msub").tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode("mdiag -j {job_id} --xml").tag(config=True)
    batch_cancel_cmd = Unicode("mjobctl -c {job_id}").tag(config=True)
    state_pending_re = Unicode(r'State="Idle"').tag(config=True)
    state_running_re = Unicode(r'State="Running"').tag(config=True)
    state_exechost_re = Unicode(r'AllocNodeList="([^\r\n\t\f :"]*)').tag(config=True)


class PBSSpawner(TorqueSpawner):
    batch_script = Unicode(
        """#!/bin/sh
{% if queue or host %}#PBS -q {% if queue  %}{{queue}}{% endif %}\
{% if host %}@{{host}}{% endif %}{% endif %}
#PBS -l walltime={{runtime}}
#PBS -l select=1:ncpus={{nprocs}}:mem={{memory}}
#PBS -N jupyterhub-singleuser
#PBS -o {{homedir}}/.jupyterhub.pbs.out
#PBS -e {{homedir}}/.jupyterhub.pbs.err
#PBS -v {{keepvars}}
{% if options %}#PBS {{options}}{% endif %}

set -eu

{{prologue}}
{{cmd}}
{{epilogue}}
"""
    ).tag(config=True)

    # outputs job data XML string
    batch_query_cmd = Unicode("qstat -fx {job_id}").tag(config=True)

    state_pending_re = Unicode(r"job_state = [QH]").tag(config=True)
    state_running_re = Unicode(r"job_state = R").tag(config=True)
    state_exechost_re = Unicode(r"exec_host = ([\w_-]+)/").tag(config=True)


class UserEnvMixin:
    """Mixin class that computes values for USER, SHELL and HOME in the environment passed to
    the job submission subprocess in case the batch system needs these for the batch script.
    """

    def user_env(self, env):
        """get user environment"""
        env["USER"] = self.user.name
        home = pwd.getpwnam(self.user.name).pw_dir
        shell = pwd.getpwnam(self.user.name).pw_shell
        if home:
            env["HOME"] = home
        if shell:
            env["SHELL"] = shell
        return env

    def get_env(self):
        """Get user environment variables to be passed to the user's job

        Everything here should be passed to the user's job as
        environment.  Caution: If these variables are used for
        authentication to the batch system commands as an admin, be
        aware that the user will receive access to these as well.
        """
        env = super().get_env()
        env = self.user_env(env)
        return env


class SlurmSpawner(UserEnvMixin, BatchSpawnerRegexStates):
    batch_script = Unicode(
        """#!/bin/bash
#SBATCH --output={{homedir}}/jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
#SBATCH --chdir={{homedir}}
#SBATCH --export={{keepvars}}
#SBATCH --get-user-env=L
{% if partition  %}#SBATCH --partition={{partition}}
{% endif %}{% if runtime    %}#SBATCH --time={{runtime}}
{% endif %}{% if memory     %}#SBATCH --mem={{memory}}
{% endif %}{% if gres       %}#SBATCH --gres={{gres}}
{% endif %}{% if nprocs     %}#SBATCH --cpus-per-task={{nprocs}}
{% endif %}{% if reservation%}#SBATCH --reservation={{reservation}}
{% endif %}{% if options    %}#SBATCH {{options}}{% endif %}

set -euo pipefail

trap 'echo SIGTERM received' TERM
{{prologue}}
which jupyterhub-singleuser
{% if srun %}{{srun}} {% endif %}{{cmd}}
echo "jupyterhub-singleuser ended gracefully"
{{epilogue}}
"""
    ).tag(config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_cluster = Unicode(
        "",
        help="Cluster name to submit job to resource manager",
    ).tag(config=True)

    req_qos = Unicode(
        "",
        help="QoS name to submit job to resource manager",
    ).tag(config=True)

    req_srun = Unicode(
        "srun",
        help="Set req_srun='' to disable running in job step, and note that "
        "this affects environment handling.  This is effectively a "
        "prefix for the singleuser command.",
    ).tag(config=True)

    req_reservation = Unicode(
        "",
        help="Reservation name to submit to resource manager",
    ).tag(config=True)

    req_gres = Unicode(
        "",
        help="Additional resources (e.g. GPUs) requested",
    ).tag(config=True)

    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode("sbatch --parsable").tag(config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode("squeue -h -j {job_id} -o '%T %B'").tag(config=True)
    batch_cancel_cmd = Unicode("scancel {job_id}").tag(config=True)
    # use long-form states: PENDING,  CONFIGURING = pending
    #  RUNNING,  COMPLETING = running
    state_pending_re = Unicode(r"^(?:PENDING|CONFIGURING)").tag(config=True)
    state_running_re = Unicode(r"^(?:RUNNING|COMPLETING)").tag(config=True)
    state_unknown_re = Unicode(
        r"^slurm_load_jobs error: (?:Socket timed out on send/recv|Unable to contact slurm controller)"
    ).tag(config=True)
    state_exechost_re = Unicode(r"\s+((?:[\w_-]+\.?)+)$").tag(config=True)

    def parse_job_id(self, output):
        # make sure jobid is really a number
        try:
            # use only last line to circumvent slurm bug
            output = output.splitlines()[-1]
            id = output.split(";")[0]
            int(id)
        except Exception as e:
            self.log.error("SlurmSpawner unable to parse job ID from text: " + output)
            raise e
        return id


class MultiSlurmSpawner(SlurmSpawner):
    """When slurm has been compiled with --enable-multiple-slurmd, the
    administrator sets the name of the slurmd instance via the slurmd -N
    option. This node name is usually different from the hostname and may
    not be resolvable by JupyterHub. Here we enable the administrator to
    map the node names onto the real hostnames via a traitlet."""

    daemon_resolver = Dict(
        {},
        help="Map node names to hostnames",
    ).tag(config=True)

    def state_gethost(self):
        host = SlurmSpawner.state_gethost(self)
        return self.daemon_resolver.get(host, host)


class GridengineSpawner(BatchSpawnerBase):
    batch_script = Unicode(
        """#!/bin/bash
#$ -j yes
#$ -N spawner-jupyterhub
#$ -o {homedir}/.jupyterhub.sge.out
#$ -e {homedir}/.jupyterhub.sge.err
#$ -v {keepvars}
#$ {options}

set -euo pipefail

{prologue}
{cmd}
{epilogue}
"""
    ).tag(config=True)

    # outputs job id string
    batch_submit_cmd = Unicode("qsub").tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode("qstat -xml").tag(config=True)
    batch_cancel_cmd = Unicode("qdel {job_id}").tag(config=True)

    def parse_job_id(self, output):
        return output.split(" ")[2]

    def state_ispending(self):
        if self.job_status:
            job_info = ET.fromstring(self.job_status).find(
                ".//job_list[JB_job_number='{0}']".format(self.job_id)
            )
            if job_info is not None:
                return job_info.attrib.get("state") == "pending"
        return False

    def state_isrunning(self):
        if self.job_status:
            job_info = ET.fromstring(self.job_status).find(
                ".//job_list[JB_job_number='{0}']".format(self.job_id)
            )
            if job_info is not None:
                return job_info.attrib.get("state") == "running"
        return False

    def state_gethost(self):
        if self.job_status:
            queue_name = ET.fromstring(self.job_status).find(
                ".//job_list[JB_job_number='{0}']/queue_name".format(self.job_id)
            )
            if queue_name is not None and queue_name.text:
                return queue_name.text.split("@")[1]

        self.log.error(
            "Spawner unable to match host addr in job {0} with status {1}".format(
                self.job_id, self.job_status
            )
        )
        return

    def get_env(self):
        env = super().get_env()

        # SGE relies on environment variables to launch local jobs. Ensure that these values are included
        # in the environment used to run the spawner.
        for key in [
            "SGE_CELL",
            "SGE_EXECD",
            "SGE_ROOT",
            "SGE_CLUSTER_NAME",
            "SGE_QMASTER_PORT",
            "SGE_EXECD_PORT",
            "PATH",
        ]:
            if key in os.environ and key not in env:
                env[key] = os.environ[key]
        return env


class CondorSpawner(UserEnvMixin, BatchSpawnerRegexStates):
    batch_script = Unicode(
        """
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
"""
    ).tag(config=True)

    # outputs job id string
    batch_submit_cmd = Unicode("condor_submit").tag(config=True)
    # outputs job data XML string
    batch_query_cmd = Unicode(
        'condor_q {job_id} -format "%s, " JobStatus -format "%s" RemoteHost -format "\n" True'
    ).tag(config=True)
    batch_cancel_cmd = Unicode("condor_rm {job_id}").tag(config=True)
    # job status: 1 = pending, 2 = running
    state_pending_re = Unicode(r"^1,").tag(config=True)
    state_running_re = Unicode(r"^2,").tag(config=True)
    state_exechost_re = Unicode(r"^\w*, .*@([^ ]*)").tag(config=True)

    def parse_job_id(self, output):
        match = re.search(r".*submitted to cluster ([0-9]+)", output)
        if match:
            return match.groups()[0]

        error_msg = "CondorSpawner unable to parse jobID from text: " + output
        self.log.error(error_msg)
        raise Exception(error_msg)

    def cmd_formatted_for_batch(self):
        return (
            super(CondorSpawner, self)
            .cmd_formatted_for_batch()
            .replace('"', '""')
            .replace("'", "''")
        )


class LsfSpawner(BatchSpawnerBase):
    """A Spawner that uses IBM's Platform Load Sharing Facility (LSF) to launch notebooks."""

    batch_script = Unicode(
        """#!/bin/sh
#BSUB -R "select[type==any]"    # Allow spawning on non-uniform hardware
#BSUB -R "span[hosts=1]"        # Only spawn job on one server
#BSUB -q {queue}
#BSUB -J spawner-jupyterhub
#BSUB -o {homedir}/.jupyterhub.lsf.out
#BSUB -e {homedir}/.jupyterhub.lsf.err

set -eu

{prologue}
{cmd}
{epilogue}
"""
    ).tag(config=True)

    batch_submit_cmd = Unicode("bsub").tag(config=True)
    batch_query_cmd = Unicode('bjobs -a -noheader -o "STAT EXEC_HOST" {job_id}').tag(
        config=True
    )
    batch_cancel_cmd = Unicode("bkill {job_id}").tag(config=True)

    def get_env(self):
        env = super().get_env()

        # LSF relies on environment variables to launch local jobs.  Ensure that these values are included
        # in the environment used to run the spawner.
        for key in [
            "LSF_ENVDIR",
            "LSF_SERVERDIR",
            "LSF_FULL_VERSION",
            "LSF_LIBDIR",
            "LSF_BINDIR",
        ]:
            if key in os.environ and key not in env:
                env[key] = os.environ[key]
        return env

    def parse_job_id(self, output):
        # Assumes output in the following form:
        # "Job <1815> is submitted to default queue <normal>."
        return output.split(" ")[1].strip("<>")

    def state_ispending(self):
        # Parse results of batch_query_cmd
        # Output determined by results of self.batch_query_cmd
        if self.job_status:
            return self.job_status.split(" ")[0].upper() in {"PEND", "PUSP"}

    def state_isrunning(self):
        if self.job_status:
            return self.job_status.split(" ")[0].upper() == "RUN"

    def state_gethost(self):
        if self.job_status:
            return self.job_status.split(" ")[1].strip().split(":")[0]

        self.log.error(
            "Spawner unable to match host addr in job {0} with status {1}".format(
                self.job_id, self.job_status
            )
        )
        return


# vim: set ai expandtab softtabstop=4:


class ARCSpawner(BatchSpawnerRegexStates):
    # TODO: new key on every connection
    # TODO: handle token

    # TODO: user dir persistence
    # TODO: image selection
    # TODO: different certs for different users

    @property
    def jh_base_url(self):
        return os.getenv("JH_BASE_URL", "https://jh-staging.cta.cscs.ch/")

    def env_string(self):
        env = ""
        for key, value in self.get_env().items():
            value = re.sub("http://.*?:8081/hub", self.jh_base_url + "/hub", value)
            # if key in ["JUPYTERHUB_SERVICE_PREFIX", "JUPYTERHUB_SERVICE_URL"]:
            #    value = value.replace("/user", "/hub/user")
            env += '("{}" ^@{}@)\n'.format(key, value)
        return env

    def user_to_path_fragment(self, user):
        if isinstance(user, dict):
            user = user["name"]

        return re.sub("[^0-1a-z]", "_", user.lower())

    def get_env(self):
        env = super().get_env()

        env["JUPYTER_PORT"] = str(self.port)
        env["JUPYTERHUB_BASE_URL"] = self.jh_base_url
        env["CTADS_URL"] = self.jh_base_url + "/services/downloadservice/"
        env["X509_USER_PROXY"] = os.environ.get(
            "X509_USER_PROXY", "/downloadservice-data/dcache_clientcert.crt"
        )
        env["SIF_IMAGE_URL"] = os.environ.get(
            "SIF_IMAGE_URL", "https://dcache.cta.cscs.ch:2880/lst/software/jh-lst-53e79bd3.sif"
        )

        if self.user.name:
            filename = self.user_to_path_fragment(self.user.name) + ".crt"
            own_certificate_file = os.path.join(
                os.environ["CTADS_CERTIFICATE_DIR"], filename
            )

            if os.path.isfile(own_certificate_file):
                env["X509_USER_PROXY"] = own_certificate_file

        return env

    @property
    def batch_script(self):
        return f"""&
                ( jobname = "session" )
                ( executable = "/usr/bin/bash" )( arguments = "run.sh" )
                ( environment = 
                    {self.env_string()}
                )
                ( inputfiles = 
                    ("run.sh" "{self.run_script_url}")
                    ("fkdata" "/etc/forwardkey")
                    ("image.sif" "{self.get_env()["SIF_IMAGE_URL"]}") 
                )

                ( outputFiles = 
                    ("user-home-tar.tgz" "https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst/users/{self.user_to_path_fragment(self.user.name)}/user-home-tar.tgz")                
                )
                    (cpuTime="4320")
                    (wallTime="4320")
                (* maximal time for the session directory to exist on the remote node, days *)
                    (lifeTime="14")
                (* memory required for the job, per count, Mbytes *)
                    (Memory="20000")
                (* disk space required for the job, Mbytes *)
                    (*Disk="100000"*)

                (priority="100")

                (count="{int(self.req_nprocs)}") 
                (countpernode="{int(self.req_nprocs)}") 

                (* (exclusiveexecution="yes") *)

                ( stdout = "stdout" )

                ( queue="normal" )

                ( join = "yes" ) 
                ( gmlog = "gmlog" ) """

    # """
    # #!/bin/bash
    # #SBATCH --output={{homedir}}/jupyterhub_slurmspawner_%j.log
    # #SBATCH --export={{keepvars}}
    # {% if partition  %}#SBATCH --partition={{partition}}
    # {% endif %}{% if runtime    %}#SBATCH --time={{runtime}}
    # {% endif %}{% if memory     %}#SBATCH --mem={{memory}}
    # {% endif %}{% if gres       %}#SBATCH --gres={{gres}}
    # {% endif %}{% if nprocs     %}#SBATCH --cpus-per-task={{nprocs}}
    # {% endif %}{% if reservation%}#SBATCH --reservation={{reservation}}
    # {% endif %}{% if options    %}#SBATCH {{options}}{% endif %}

    # set -euo pipefail

    # trap 'echo SIGTERM received' TERM
    # {{prologue}}

    # which jupyterhub-singleuser

    # {% if srun %}{{srun}} {% endif %}{{cmd}}
    # echo "jupyterhub-singleuser ended gracefully"
    # {{epilogue}}
    # """

    http_timeout = Integer(3600, config=True, help="Timeout for HTTP requests")
    start_timeout = Integer(3600, config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_cluster = Unicode(
        "",
        help="Cluster name to submit job to resource manager",
    ).tag(config=True)

    req_qos = Unicode(
        "",
        help="QoS name to submit job to resource manager",
    ).tag(config=True)

    exec_prefix = Unicode(
        "",
    ).tag(config=True)

    req_srun = Unicode(
        "srun",
        help="Set req_srun='' to disable running in job step, and note that "
        "this affects environment handling.  This is effectively a "
        "prefix for the singleuser command.",
    ).tag(config=True)

    req_reservation = Unicode(
        "",
        help="Reservation name to submit to resource manager",
    ).tag(config=True)

    req_gres = Unicode(
        "",
        help="Additional resources (e.g. GPUs) requested",
    ).tag(config=True)

    exec_prefix = Unicode(
        "",
        # "sudo -E -u {username}",
        help="Standard execution prefix (e.g. the default sudo -E -u {username})",
    ).tag(config=True)

    run_script_url = Unicode(
        "",
        # "https://dcache.cta.cscs.ch:2880/lst/software/run.sh",
        help="run_script_url",
    ).tag(config=True)

    input_as_file = True

    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode("arcsub -d DEBUG").tag(config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode("arcstat -d DEBUG {job_id}").tag(config=True)
    batch_cancel_cmd = Unicode("arckill -d DEBUG {job_id}").tag(config=True)
    # use long-form states: PENDING,  CONFIGURING = pending
    # #  RUNNING,  COMPLETING = running
    # state_pending_re = Unicode(r"^(?:Accepted|Submitted)").tag(config=True)
    # state_running_re = Unicode(r"^(?:Running)").tag(config=True)
    # state_unknown_re = Unicode(
    #     r"^_load_jobs error: (?:Socket timed out on send/recv|Unable to contact slurm controller)"
    # ).tag(config=True)
    # state_exechost_re = Unicode(r"\s+((?:[\w_-]+\.?)+)$").tag(config=True)

    def parse_job_id(self, output):
        self.log.info("output: %s", output)
        try:
            for output_line in output.splitlines():
                self.log.info("output_line: %s", output_line)
                if output_line.startswith("Job submitted with jobid:"):
                    id = output_line.split()[-1]
        except Exception as e:
            self.log.error("ARCSpawner unable to parse job ID from text: " + output)
            raise e
        return id


    async def get_arcinfo(self):
        arcinfo = subprocess.check_output(["arcinfo", "-l"]).strip()
        self.arcinfo = dict(
            free_slots=int(re.search(r"Free slots: ([0-9]*)", arcinfo.decode()).group(1)),
            total_slots=int(re.search(r"Total slots: ([0-9]*)", arcinfo.decode()).group(1)),
        )

    async def proxy_info(self):
        cmd = "arcproxy -i vomsACvalidityLeft"

        try:
            self.proxy_vomsACvalidityLeft = await self.run_command(cmd)
            self.proxy_vomsACvalidityLeft = int(self.proxy_vomsACvalidityLeft.strip())
        except RuntimeError as e:
            # e.args[0] is stderr from the process
            self.proxy_vomsACvalidityLeft = None
        except Exception as e:
            self.log.error("Error querying proxy validity: %s", e)
            self.proxy_vomsACvalidityLeft = None

    # arcinfo  -l
    
    @property
    def job_state(self):
        if self.job_status:
            if r := re.search(r"State: (.*)", self.job_status):
                return r.group(1)

    def state_ispending(self):
        # Parse results of batch_query_cmd
        # Output determined by results of self.batch_query_cmd
        # self.log.debug("\033[31mchecking pending from job_status: " + str(self.job_status) + "\033[0m")
        r = self.job_status is None or self.job_state in [
            "Accepted",
            "Submitted",
            "Queuing",
            "Preparing",
        ]
        self.log.info(
            "\033[31mstate_ispending, job state: %s pending is %s\033[0m",
            self.job_state,
            r,
        )
        return r

    def state_isrunning(self):
        # Parse results of batch_query_cmd
        # Output determined by results of self.batch_query_cmd
        r = self.job_state in ["Running"]
        self.log.info(
            "\033[31mstate_isrunning, job state: %s running is %s\033[0m",
            self.job_state,
            r,
        )
        return r

    def state_isready(self):
        if self.job_state not in ["Running"]:
            self.log.info("job state is not Running: not ready")
            return False

        if not getattr(self, "have_tunnel", False):
            self.log.info("no tunnel: not ready")
            return False

        if not hasattr(self, "host_ip"):
            self.log.info("no host_ip: not ready")
            return False

        # if not hasattr(self, 'ssh_tunnel_connection'):
        #    self.log.info("no remote ssh tunnel connection: not ready")
        #    return False

        self.log.info("job is ready")
        return True

    forward_gateway = Unicode(
        "cgw.dev.ctaodc.ch",
        help="Host used for SSH tunneling",
    ).tag(config=True)

    async def make_ssh_tunnel(self):
        self.log.info(
            "\033[32mtrying to establish ssh tunnel to %s\033[0m",
            self.forward_gateway,
            self.port,
        )
        if hasattr(self, "ssh_tunnel_connection"):
            self.log.info(
                "ssh tunnel already established: %s", self.ssh_tunnel_connection
            )
        else:
            self.log.info(
                "\033[32mestablishing ssh tunnel to %s\033[0m",
                self.forward_gateway,
                self.port,
            )
            async with asyncssh.connect(
                self.forward_gateway,
                # known_hosts="/etc/ssh_gw_known_hosts",
                known_hosts=None,
                client_keys=[
                    "/etc/forwardkey"
                    # (asyncssh.read_private_key("/etc/forwardkey"),
                    # asyncssh.read_certificate("/etc/forwardkey.pub"))
                ],
                username="forwarder",
            ) as conn:
                self.ssh_tunnel_connection = conn
                self.log.info(
                    "SSH connection established: %s will make forward to %s",
                    conn,
                    self.port,
                )
                listener = await conn.forward_local_port(
                    "0.0.0.0", self.port, "0.0.0.0", self.port
                )
                # listener = await conn.forward_local_port('127.0.0.1', self.port, '127.0.0.1', self.port)
                self.log.info(
                    "\033[31mListening %s on port %s\033[0m",
                    listener,
                    listener.get_port(),
                )
                await listener.wait_closed()
                # self.log.info("SSH connection closed: %s", conn)
                # del self.ssh_tunnel_connection

            # try:
            #     asyncio.get_event_loop().run_until_complete(run_client())
        # except (OSError, asyncssh.Error) as exc:
        #     sys.exit('SSH connection failed: ' + str(exc))

    def state_gethost(self):
        if (r := re.search(r"JPORT=(\d{4,5})", self.job_log)) is not None:
            self.anticipated_port = int(r.group(1))
            self.log.info("found anticipated port: %s", self.anticipated_port)

        r = re.search(
            r"inet (\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})/\d+? scope global hsn0",
            self.job_log,
        )
        if r is not None:
            self.host_ip = r.group(1)

        if re.search("Sending command: .*sleep 60", self.job_log):
            self.log.info("found sleep command in job log")
            self.have_tunnel = True

        server_details = re.search(
            r"http://127.0.0.1:(?P<port>\d{4})/.*?/lab(?:\?token=(?P<token>[0-9a-z]*))?",
            self.job_log,
        )

        if server_details is None:
            self.log.info(
                "no server record found in '\033[33m%s\033[0m",
                "\n".join(self.job_log.split("\n")[-30:]),
            )
            return None
        else:
            self.port = int(server_details.group("port"))
            self.server_token = server_details.group("token")

            self.log.info("found server token: %s", self.server_token)

            # return self.forward_gateway
            return "127.0.0.1"

    @default("req_homedir")
    def _req_homedir_default(self):
        return "/root"

    async def query_job_log(self):
        """Check job status, return JobStatus object."""

        cmd = f"arccat {self.job_id}"

        self.log.info("Spawner querying job: " + cmd)
        try:
            self.job_log = await self.run_command(cmd)
        except RuntimeError as e:
            # e.args[0] is stderr from the process
            self.job_log = e.args[0]
        except Exception as e:
            self.log.error("Error querying job " + self.job_id)
            self.job_log = ""

    @async_generator
    async def progress(self):
        while True:
            if self.state_ispending():
                # TODO: report cluster status and user details (dteam, etc)
                await yield_(
                    {
                        "message": (
                            f"Pending in queue, ARC status {self.job_state}"
                            f" (timeout in {int(self.start_timeout - time.time() + self.tstart)})"
                            f" Noir access valid for {self.proxy_vomsACvalidityLeft/3600:.1f}h"
                            f" current load {self.arcinfo['total_slots'] - self.arcinfo['free_slots']}/{self.arcinfo['total_slots']}"
                        )
                    }
                )
            elif self.state_isrunning():
                if self.state_isready():
                    await yield_(
                        {
                            "message": ("Cluster job started... waiting to connect"),
                        }
                    )
                    return
                else:
                    await yield_(
                        {
                            "message": (
                                "Cluster job running... waiting to see signs of life;"
                                " host "
                                + (
                                    " ASSIGNED"
                                    if hasattr(self, "host_ip")
                                    else " NOT assigned"
                                )
                                + " "
                                # f" host {getattr(self, 'host_ip', 'unknown')}"
                                " tunnel"
                                + (
                                    " READY"
                                    if getattr(self, "have_tunnel", False)
                                    else " NOT ready"
                                )
                                + " "
                                " tunnel port"
                                + (
                                    " PLANNED"
                                    if hasattr(self, "anticipated_port")
                                    else " NOT planned yet"
                                )
                            ),
                        }
                    )

            else:
                await yield_(
                    {
                        "message": "Unknown status...",
                    }
                )
            await gen.sleep(1)

    async def start(self):
        """Start the process"""

        await self.proxy_info()
        if self.proxy_vomsACvalidityLeft is None:
            raise RuntimeError(
                "No valid credentials to connect to ARC, aborting. Please contact support if you need it urgently."
            )        

        self.log.info("proxy validity: %s", self.proxy_vomsACvalidityLeft)        

        self.ip = self.traits()["ip"].default_value
        # self.port = self.traits()["port"].default_value

        self.port = random.randint(8700, 8999)

        self.tstart = time.time()

        if self.server:
            self.server.port = self.port

        self.log.info("\033[31mwill create tunnel task\033[0m")
        # await self.make_ssh_tunnel()
        self.ssh_tunnel_task = asyncio.create_task(self.make_ssh_tunnel())
        self.log.info("\033[31mcreated tunnel task %s\033[0m", self.ssh_tunnel_task)

        job = await self.submit_batch_script()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter batch job submission failure (no jobid in output)"
            )
        while True:
            self.log.info("\033[33mloop before running job\033[0m")

            await self.get_arcinfo()

            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                self.log.info(
                    "\033[33mBREAKING loop before running job since it's running\033[0m"
                )
                break
            elif status == JobStatus.PENDING:
                self.log.debug("Job " + self.job_id + " still pending")
            elif status == JobStatus.UNKNOWN:
                self.log.debug("Job " + self.job_id + " still unknown")
            else:
                self.log.warning(
                    "Job "
                    + self.job_id
                    + " neither pending nor running.\n"
                    + self.job_status
                )
                self.clear_state()
                raise RuntimeError(
                    "The Jupyter batch job has disappeared"
                    " while pending in the queue or died immediately"
                    " after starting."
                )
            await gen.sleep(self.startup_poll_interval)

        while True:
            self.log.info("\033[31mloop for running job\033[0m")
            await self.query_job_log()

            try:
                self.state_gethost()
                if self.port != self.traits()["port"].default_value:
                    self.log.info(f"found ip {self.ip} and port {self.port} in log")

                    if hasattr(self, "anticipated_port"):
                        self.log.info(
                            "\033[31mfound anticipated port: %s, will make remote ssh tunnel\033[0m",
                            self.anticipated_port,
                        )
                        break
                    else:
                        self.log.info(
                            "\033[31mno anticipated port yet, can not make remote tunnel\033[0m"
                        )
            except Exception as e:
                self.log.warn(f"failed to get ip: {e}")
            else:
                self.log.info("no ip in log yet")

            await gen.sleep(self.startup_poll_interval)

        # self.ip = "127.0.0.1" #self.state_gethost()
        self.ip = "148.187.151.63"  # self.state_gethost()
        self.port = self.anticipated_port
        self.server.port = self.anticipated_port

        while self.port == 0:
            await gen.sleep(self.startup_poll_interval)
            # Test framework: For testing, mock_port is set because we
            # don't actually run the single-user server yet.
            if hasattr(self, "mock_port"):
                self.port = self.mock_port

        self.db.commit()
        self.log.info(
            "Notebook server job {0} started at {1}:{2}".format(
                self.job_id, self.ip, self.port
            )
        )

        return self.ip, self.port
        # url = f"http://{self.ip}:{self.port}/lab?token={self.server_token}"

        # self.log.info("Spawner started job with full URL: " + url)

        # return url

    async def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_cancel_cmd, **subvars),
            )
        )
        self.log.info("Cancelling job " + self.job_id + ": " + cmd)
        await self.run_command(cmd)
        self.ssh_tunnel_task.cancel()
