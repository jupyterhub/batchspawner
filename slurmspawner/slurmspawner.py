"""SlurmSpawner implementation"""
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
#SBATCH -o jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
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
    hours = '1'
    full_cmd = cmd.split(';')
    export_cmd = full_cmd[0]
    cmd = full_cmd[1]
    sbatch = sbatch.substitute(dict(export_cmd=export_cmd, cmd=cmd, queue=queue, mem=mem, hours=hours, user=user))
    #serialsbatch+='cd %s' % "notebooks"
    print('Submitting *****{\n%s\n}*****' % sbatch)
    popen = subprocess.Popen('sbatch', shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE)
    out = popen.communicate(sbatch.encode())[0].strip() #e.g. something like "Submitted batch job 209"
    return out

class SlurmSpawner(Spawner):
    """A Spawner that just uses Popen to start local processes."""

    INTERRUPT_TIMEOUT = Integer(1200, config=True, \
        help="Seconds to wait for process to halt after SIGINT before proceeding to SIGTERM"
                               )
    TERM_TIMEOUT = Integer(1200, config=True, \
        help="Seconds to wait for process to halt after SIGTERM before proceeding to SIGKILL"
                          )
    KILL_TIMEOUT = Integer(1200, config=True, \
        help="Seconds to wait for process to halt after SIGKILL before giving up"
                          )

    ip = Unicode("0.0.0.0", config=True, \
        help="url of the server")

    server_user = Unicode(getpass.getuser(), config=True, \
        help="user who is logged in on the server")

    slurm_job_id = Unicode() # will get populated after spawned

    pid = Integer(0)

    def make_preexec_fn(self, name):
        """make preexec fn"""
        return set_user_setuid(name)

    def load_state(self, state):
        """load slurm_job_id from state"""
        super(SlurmSpawner, self).load_state(state)
        self.slurm_job_id = state.get('slurm_job_id', '')

    def get_state(self):
        """add slurm_job_id to state"""
        state = super(SlurmSpawner, self).get_state()
        if self.slurm_job_id:
            state['slurm_job_id'] = self.slurm_job_id
        return state

    def clear_state(self):
        """clear slurm_job_id state"""
        super(SlurmSpawner, self).clear_state()
        self.slurm_job_id = ""

    def user_env(self, env):
        """get user environment"""
        env['USER'] = self.user.name
        env['HOME'] = pwd.getpwnam(self.user.name).pw_dir
        return env

    def _env_default(self):
        env = super()._env_default()
        return self.user_env(env)

    def _check_slurm_job_state(self):
        # check sacct to see if the job is still running
        cmd = 'sacct -n -j ' + self.slurm_job_id + ' -o state'
        out = run_command(cmd)
        self.log.info("Notebook server for user %s: Slurm jobid %s status: %s" % (self.user.name, self.slurm_job_id, out))
        if "RUNNING" in out:
            return None
        else:
            return 1

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
        
        ####################################################################################
        # These 2 lines causing problems with logging into the hub - can't log back in once logged out
        #user = self.make_preexec_fn(self.user.name)
        #user()
        #####################################################################################
        
        output = run_jupyterhub_singleuser(' '.join(cmd), self.user.name)
        output = output.decode() # convert bytes object to string
        self.log.info(output)
        self.slurm_job_id = output.split(' ')[-1] # the job id should be the very last part of the string

        # make sure jobid is really a number
        try:
            int(self.slurm_job_id)
        except ValueError:
            self.log.info("sbatch returned this at the end of their string: %s" % self.slurm_job_id)

        #time.sleep(2)
        job_state = self._check_slurm_job_state()
        for i in range(10):
            if job_state is not None:
                self.log.info("job_state is %s" % job_state)
                job_state = self._check_slurm_job_state()
                time.sleep(1)
            else:
                break
        
        notebook_ip = get_slurm_job_info(self.slurm_job_id)

        self.user.server.ip = notebook_ip # don't know if this is right or not
        self.log.info("Notebook server ip is %s" % self.user.server.ip)

    @gen.coroutine
    def poll(self):
        """Poll the process"""
        return self._check_slurm_job_state()

    @gen.coroutine
    def _signal(self, sig):
        """simple implementation of signal

        we can use it when we are using setuid (we are root)"""
        #try:
        #    os.kill(self.pid, sig)
        #except OSError as e:
        #    if e.errno == errno.ESRCH:
        #        return False # process is gone
        #    else:
        #        raise
        return True # process exists

    @gen.coroutine
    def stop(self, now=False):
        """stop the subprocess

        if `now`, skip waiting for clean shutdown
        """
        cmd = 'scancel ' + self.slurm_job_id
        self.log.info("cancelling job %s" % self.slurm_job_id)
        if now:
            return

        out = run_command(cmd)

        if out in ("CANCELLED", "COMPLETED", "FAILED"):
            return
        while True:
            status = self.poll()
            if status is None:
                continue
            break
        # means job is no longer running, so return
        return


if __name__ == "__main__":

        run_jupyterhub_singleuser("jupyterhub-singleuser", 3434)
