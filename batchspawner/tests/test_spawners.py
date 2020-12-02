"""Test BatchSpawner and subclasses"""

import re
from unittest import mock
from .. import BatchSpawnerRegexStates, JobStatus
from traitlets import Unicode
import time
import pytest
from jupyterhub import orm, version_info
from tornado import gen

try:
    from jupyterhub.objects import Hub, Server
    from jupyterhub.user import User
except:
    pass

testhost = "userhost123"
testjob  = "12345"
testport = 54321

class BatchDummy(BatchSpawnerRegexStates):
    exec_prefix = ''
    batch_submit_cmd = Unicode('cat > /dev/null; echo '+testjob)
    batch_query_cmd = Unicode('echo RUN '+testhost)
    batch_cancel_cmd = Unicode('echo STOP')
    batch_script = Unicode('{cmd}')
    state_pending_re = Unicode('PEND')
    state_running_re = Unicode('RUN')
    state_exechost_re = Unicode('RUN (.*)$')
    state_unknown_re = Unicode('UNKNOWN')

    cmd_expectlist = None
    out_expectlist = None
    def run_command(self, *args, **kwargs):
        """Overwriten run command to test templating and outputs"""
        cmd = args[0]
        # Test that the command matches the expectations
        if self.cmd_expectlist:
            run_re = self.cmd_expectlist.pop(0)
            if run_re:
                print('run:', run_re)
                assert run_re.search(cmd) is not None, \
                  "Failed test: re={0} cmd={1}".format(run_re, cmd)
        # Run command normally
        out = super().run_command(*args, **kwargs)
        # Test that the command matches the expectations
        if self.out_expectlist:
            out_re = self.out_expectlist.pop(0)
            if out_re:
                print('out:', out_re)
                assert out_re.search(cmd) is not None, \
                  "Failed output: re={0} cmd={1} out={2}".format(out_re, cmd, out)
        return out

def new_spawner(db, spawner_class=BatchDummy, **kwargs):
    kwargs.setdefault('cmd', ['singleuser_command'])
    user = db.query(orm.User).first()
    if version_info < (0,8):
        hub = db.query(orm.Hub).first()
    else:
        hub = Hub()
        user = User(user, {})
        server = Server()
        # Set it after constructions because it isn't a traitlet.
    kwargs.setdefault('hub', hub)
    kwargs.setdefault('user', user)
    kwargs.setdefault('poll_interval', 1)
    if version_info < (0,8):
        spawner = spawner_class(db=db, **kwargs)
        spawner.mock_port = testport
    else:
        print("JupyterHub >=0.8 detected, using new spawner creation")
        # These are not traitlets so we have to set them here
        spawner = user._new_spawner('', spawner_class=spawner_class, **kwargs)
        spawner.server = server
        spawner.mock_port = testport
    return spawner

@pytest.mark.slow
def test_stress_submit(db, io_loop):
    for i in range(200):
        time.sleep(0.01)
        test_spawner_start_stop_poll(db, io_loop)

def check_ip(spawner, value):
    if version_info < (0,7):
        assert spawner.user.server.ip == value
    else:
        assert spawner.ip == value

def test_spawner_start_stop_poll(db, io_loop):
    spawner = new_spawner(db=db)

    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    assert spawner.job_id == ''
    assert spawner.get_state() == {}

    io_loop.run_sync(spawner.start, timeout=5)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status is None
    spawner.batch_query_cmd = 'echo NOPE'
    io_loop.run_sync(spawner.stop, timeout=5)
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    assert spawner.get_state() == {}

def test_spawner_state_reload(db, io_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}

    io_loop.run_sync(spawner.start, timeout=30)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    state = spawner.get_state()
    assert state == dict(job_id=testjob, job_status='RUN '+testhost)
    spawner = new_spawner(db=db)
    spawner.clear_state()
    assert spawner.get_state() == {}
    spawner.load_state(state)
    # We used to check IP here, but that is actually only computed on start(),
    # and is not part of the spawner's persistent state
    assert spawner.job_id == testjob

def test_submit_failure(db, io_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_submit_cmd = 'cat > /dev/null; true'
    with pytest.raises(RuntimeError) as e_info:
        io_loop.run_sync(spawner.start, timeout=30)
    assert spawner.job_id == ''
    assert spawner.job_status == ''

def test_submit_pending_fails(db, io_loop):
    """Submission works, but the batch query command immediately fails"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_query_cmd = 'echo xyz'
    with pytest.raises(RuntimeError) as e_info:
        io_loop.run_sync(spawner.start, timeout=30)
    status = io_loop.run_sync(spawner.query_job_status, timeout=30)
    assert status == JobStatus.NOTFOUND
    assert spawner.job_id == ''
    assert spawner.job_status == ''

def test_poll_fails(db, io_loop):
    """Submission works, but a later .poll() fails"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    # The start is successful:
    io_loop.run_sync(spawner.start, timeout=30)
    spawner.batch_query_cmd = 'echo xyz'
    # Now, the poll fails:
    io_loop.run_sync(spawner.poll, timeout=30)
    # .poll() will run self.clear_state() if it's not found:
    assert spawner.job_id == ''
    assert spawner.job_status == ''

def test_unknown_status(db, io_loop):
    """Polling returns an unknown status"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    # The start is successful:
    io_loop.run_sync(spawner.start, timeout=30)
    spawner.batch_query_cmd = 'echo UNKNOWN'
    # This poll should not fail:
    io_loop.run_sync(spawner.poll, timeout=30)
    status = io_loop.run_sync(spawner.query_job_status, timeout=30)
    assert status == JobStatus.UNKNOWN
    assert spawner.job_id == '12345'
    assert spawner.job_status != ''


def test_templates(db, io_loop):
    """Test templates in the run_command commands"""
    spawner = new_spawner(db=db)

    # Test when not running
    spawner.cmd_expectlist = [re.compile('.*RUN'),
                             ]
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    assert spawner.job_id == ''
    assert spawner.get_state() == {}

    # Test starting
    spawner.cmd_expectlist = [re.compile('.*echo'),
                              re.compile('.*RUN'),
                             ]
    io_loop.run_sync(spawner.start, timeout=5)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    # Test poll - running
    spawner.cmd_expectlist = [re.compile('.*RUN'),
                             ]
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status is None

    # Test stopping
    spawner.batch_query_cmd = 'echo NOPE'
    spawner.cmd_expectlist = [re.compile('.*STOP'),
                              re.compile('.*NOPE'),
                             ]
    io_loop.run_sync(spawner.stop, timeout=5)
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    assert spawner.get_state() == {}

def test_batch_script(db, io_loop):
    """Test that the batch script substitutes {cmd}"""
    class BatchDummyTestScript(BatchDummy):
        @gen.coroutine
        def _get_batch_script(self, **subvars):
            script = yield super()._get_batch_script(**subvars)
            assert 'singleuser_command' in script
            return script
    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript)
    #status = io_loop.run_sync(spawner.poll, timeout=5)
    io_loop.run_sync(spawner.start, timeout=5)
    #status = io_loop.run_sync(spawner.poll, timeout=5)
    #io_loop.run_sync(spawner.stop, timeout=5)

def test_exec_prefix(db, io_loop):
    """Test that all run_commands have exec_prefix"""
    class BatchDummyTestScript(BatchDummy):
        exec_prefix = 'PREFIX'
        @gen.coroutine
        def run_command(self, cmd, *args, **kwargs):
            assert cmd.startswith('PREFIX ')
            cmd = cmd[7:]
            print(cmd)
            out = yield super().run_command(cmd, *args, **kwargs)
            return out
    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript)
    # Not running
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    # Start
    io_loop.run_sync(spawner.start, timeout=5)
    assert spawner.job_id == testjob
    # Poll
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status is None
    # Stop
    spawner.batch_query_cmd = 'echo NOPE'
    io_loop.run_sync(spawner.stop, timeout=5)
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1

def run_spawner_script(db, io_loop, spawner, script,
                       batch_script_re_list=None, spawner_kwargs={}):
    """Run a spawner script and test that the output and behavior is as expected.

    db: same as in this module
    io_loop: same as in this module
    spawner: the BatchSpawnerBase subclass to test
    script: list of (input_re_to_match, output)
    batch_script_re_list: if given, assert batch script matches all of these
    """
    # Create the expected scripts
    cmd_expectlist, out_list = zip(*script)
    cmd_expectlist = list(cmd_expectlist)
    out_list = list(out_list)

    class BatchDummyTestScript(spawner):
        @gen.coroutine
        def run_command(self, cmd, input=None, env=None):
            # Test the input
            run_re = cmd_expectlist.pop(0)
            if run_re:
                print('run: "{}"   [{}]'.format(cmd, run_re))
                assert run_re.search(cmd) is not None, \
                  "Failed test: re={0} cmd={1}".format(run_re, cmd)
            # Test the stdin - will only be the batch script.  For
            # each regular expression in batch_script_re_list, assert that
            # each re in that list matches the batch script.
            if batch_script_re_list and input:
                batch_script = input
                for match_re in batch_script_re_list:
                    assert match_re.search(batch_script) is not None, \
                      "Batch script does not match {}".format(match_re)
            # Return expected output.
            out = out_list.pop(0)
            print('  --> '+out)
            return out

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript,
                          **spawner_kwargs)
    # Not running at beginning (no command run)
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    # batch_submit_cmd
    # batch_query_cmd    (result=pending)
    # batch_query_cmd    (result=running)
    io_loop.run_sync(spawner.start, timeout=5)
    assert spawner.job_id == testjob
    check_ip(spawner, testhost)
    # batch_query_cmd
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status is None
    # batch_cancel_cmd
    io_loop.run_sync(spawner.stop, timeout=5)
    # batch_poll_cmd
    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1



def test_torque(db, io_loop):
    spawner_kwargs = {
        'req_nprocs': '5',
        'req_memory': '5678',
        'req_options': 'some_option_asdf',
        'req_prologue': 'PROLOGUE',
        'req_epilogue': 'EPILOGUE',
        }
    batch_script_re_list = [
        re.compile(r'^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE', re.S|re.M),
        re.compile(r'mem=5678'),
        re.compile(r'ppn=5'),
        re.compile(r'^#PBS some_option_asdf', re.M),
        ]
    script = [
        (re.compile(r'sudo.*qsub'), str(testjob)),
        (re.compile(r'sudo.*qstat'),  '<job_state>Q</job_state><exec_host></exec_host>'.format(testhost)),   # pending
        (re.compile(r'sudo.*qstat'),  '<job_state>R</job_state><exec_host>{}/1</exec_host>'.format(testhost)),  # running
        (re.compile(r'sudo.*qstat'),  '<job_state>R</job_state><exec_host>{}/1</exec_host>'.format(testhost)),  # running
        (re.compile(r'sudo.*qdel'),   'STOP'),
        (re.compile(r'sudo.*qstat'),  ''),
        ]
    from .. import TorqueSpawner
    run_spawner_script(db, io_loop, TorqueSpawner, script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)


def test_moab(db, io_loop):
    spawner_kwargs = {
        'req_nprocs': '5',
        'req_memory': '5678',
        'req_options': 'some_option_asdf',
        'req_prologue': 'PROLOGUE',
        'req_epilogue': 'EPILOGUE',
        }
    batch_script_re_list = [
        re.compile(r'^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE', re.S|re.M),
        re.compile(r'mem=5678'),
        re.compile(r'ppn=5'),
        re.compile(r'^#PBS some_option_asdf', re.M),
        ]
    script = [
        (re.compile(r'sudo.*msub'), str(testjob)),
        (re.compile(r'sudo.*mdiag'),  'State="Idle"'.format(testhost)),   # pending
        (re.compile(r'sudo.*mdiag'),  'State="Running" AllocNodeList="{}"'.format(testhost)),  # running
        (re.compile(r'sudo.*mdiag'),  'State="Running" AllocNodeList="{}"'.format(testhost)),  # running
        (re.compile(r'sudo.*mjobctl.*-c'),   'STOP'),
        (re.compile(r'sudo.*mdiag'),  ''),
        ]
    from .. import MoabSpawner
    run_spawner_script(db, io_loop, MoabSpawner, script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)


def test_pbs(db, io_loop):
    spawner_kwargs = {
        'req_nprocs': '4',
        'req_memory': '10256',
        'req_options': 'some_option_asdf',
        'req_host': 'some_pbs_admin_node',
        'req_runtime': '08:00:00',
        }
    batch_script_re_list = [
        re.compile(r'singleuser_command'),
        re.compile(r'select=1'),
        re.compile(r'ncpus=4'),
        re.compile(r'mem=10256'),
        re.compile(r'walltime=08:00:00'),
        re.compile(r'@some_pbs_admin_node'),
        re.compile(r'^#PBS some_option_asdf', re.M),
        ]
    script = [
        (re.compile(r'sudo.*qsub'), str(testjob)),
        (re.compile(r'sudo.*qstat'),  'job_state = Q'.format(testhost)),   # pending
        (re.compile(r'sudo.*qstat'),  'job_state = R\nexec_host = {}/2*1'.format(testhost)),  # running
        (re.compile(r'sudo.*qstat'),  'job_state = R\nexec_host = {}/2*1'.format(testhost)),  # running
        (re.compile(r'sudo.*qdel'),   'STOP'),
        (re.compile(r'sudo.*qstat'),  ''),
        ]
    from .. import PBSSpawner
    run_spawner_script(db, io_loop, PBSSpawner, script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)


def test_slurm(db, io_loop):
    spawner_kwargs = {
        'req_runtime': '3-05:10:10',
        'req_nprocs': '5',
        'req_memory': '5678',
        'req_options': 'some_option_asdf',
        'req_prologue': 'PROLOGUE',
        'req_epilogue': 'EPILOGUE',
        'req_reservation': 'RES123',
        'req_gres': 'GRES123',
        }
    batch_script_re_list = [
        re.compile(r'PROLOGUE.*srun batchspawner-singleuser singleuser_command.*EPILOGUE', re.S),
        re.compile(r'^#SBATCH \s+ --cpus-per-task=5', re.X|re.M),
        re.compile(r'^#SBATCH \s+ --time=3-05:10:10', re.X|re.M),
        re.compile(r'^#SBATCH \s+ some_option_asdf', re.X|re.M),
        re.compile(r'^#SBATCH \s+ --reservation=RES123', re.X|re.M),
        re.compile(r'^#SBATCH \s+ --gres=GRES123', re.X|re.M),
        ]
    from .. import SlurmSpawner
    run_spawner_script(db, io_loop, SlurmSpawner, normal_slurm_script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)
# We tend to use slurm as our typical example job.  These allow quick
# Slurm examples.
normal_slurm_script = [
        (re.compile(r'sudo.*sbatch'),   str(testjob)),
        (re.compile(r'sudo.*squeue'),   'PENDING '),          # pending
        (re.compile(r'sudo.*squeue'),   'slurm_load_jobs error: Unable to contact slurm controller'), # unknown
        (re.compile(r'sudo.*squeue'),   'RUNNING '+testhost), # running
        (re.compile(r'sudo.*squeue'),   'RUNNING '+testhost),
        (re.compile(r'sudo.*scancel'),  'STOP'),
        (re.compile(r'sudo.*squeue'),   ''),
        ]
from .. import SlurmSpawner
def run_typical_slurm_spawner(db, io_loop,
        spawner=SlurmSpawner,
        script=normal_slurm_script,
        batch_script_re_list=None,
        spawner_kwargs={}):
    """Run a full slurm job with default (overrideable) parameters.

    This is useful, for example, for changing options and testing effect
    of batch scripts.
    """
    return run_spawner_script(db, io_loop, spawner, script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)


#def test_gridengine(db, io_loop):
#    spawner_kwargs = {
#        'req_options': 'some_option_asdf',
#        }
#    batch_script_re_list = [
#        re.compile(r'singleuser_command'),
#        re.compile(r'#$\s+some_option_asdf'),
#        ]
#    script = [
#        (re.compile(r'sudo.*qsub'),   'x x '+str(testjob)),
#        (re.compile(r'sudo.*qstat'),   'PENDING '),
#        (re.compile(r'sudo.*qstat'),   'RUNNING '+testhost),
#        (re.compile(r'sudo.*qstat'),   'RUNNING '+testhost),
#        (re.compile(r'sudo.*qdel'),  'STOP'),
#        (re.compile(r'sudo.*qstat'),   ''),
#        ]
#    from .. import GridengineSpawner
#    run_spawner_script(db, io_loop, GridengineSpawner, script,
#                       batch_script_re_list=batch_script_re_list,
#                       spawner_kwargs=spawner_kwargs)


def test_condor(db, io_loop):
    spawner_kwargs = {
        'req_nprocs': '5',
        'req_memory': '5678',
        'req_options': 'some_option_asdf',
        }
    batch_script_re_list = [
        re.compile(r'exec batchspawner-singleuser singleuser_command'),
        re.compile(r'RequestCpus = 5'),
        re.compile(r'RequestMemory = 5678'),
        re.compile(r'^some_option_asdf', re.M),
        ]
    script = [
        (re.compile(r'sudo.*condor_submit'),   'submitted to cluster {}'.format(str(testjob))),
        (re.compile(r'sudo.*condor_q'),   '1,'.format(testhost)),  # pending
        (re.compile(r'sudo.*condor_q'),   '2, @{}'.format(testhost)),  # runing
        (re.compile(r'sudo.*condor_q'),   '2, @{}'.format(testhost)),
        (re.compile(r'sudo.*condor_rm'),  'STOP'),
        (re.compile(r'sudo.*condor_q'),   ''),
        ]
    from .. import CondorSpawner
    run_spawner_script(db, io_loop, CondorSpawner, script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)


def test_lfs(db, io_loop):
    spawner_kwargs = {
        'req_nprocs': '5',
        'req_memory': '5678',
        'req_options': 'some_option_asdf',
        'req_queue': 'some_queue',
        'req_prologue': 'PROLOGUE',
        'req_epilogue': 'EPILOGUE',
        }
    batch_script_re_list = [
        re.compile(r'^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE', re.S|re.M),
        re.compile(r'#BSUB\s+-q\s+some_queue', re.M),
        ]
    script = [
        (re.compile(r'sudo.*bsub'),   'Job <{}> is submitted to default queue <normal>'.format(str(testjob))),
        (re.compile(r'sudo.*bjobs'),  'PEND '.format(testhost)),  # pending
        (re.compile(r'sudo.*bjobs'),  'RUN {}'.format(testhost)),  # running
        (re.compile(r'sudo.*bjobs'),  'RUN {}'.format(testhost)),
        (re.compile(r'sudo.*bkill'),  'STOP'),
        (re.compile(r'sudo.*bjobs'),  ''),
        ]
    from .. import LsfSpawner
    run_spawner_script(db, io_loop, LsfSpawner, script,
                       batch_script_re_list=batch_script_re_list,
                       spawner_kwargs=spawner_kwargs)


def test_keepvars(db, io_loop):
    # req_keepvars
    spawner_kwargs = {
        'req_keepvars': 'ABCDE',
        }
    batch_script_re_list = [
        re.compile(r'--export=ABCDE', re.X|re.M),
        ]
    run_typical_slurm_spawner(db, io_loop,
                              spawner_kwargs=spawner_kwargs,
                              batch_script_re_list=batch_script_re_list)

    # req_keepvars AND req_keepvars together
    spawner_kwargs = {
        'req_keepvars': 'ABCDE',
        'req_keepvars_extra': 'XYZ',
        }
    batch_script_re_list = [
        re.compile(r'--export=ABCDE,XYZ', re.X|re.M),
        ]
    run_typical_slurm_spawner(db, io_loop,
                              spawner_kwargs=spawner_kwargs,
                              batch_script_re_list=batch_script_re_list)
