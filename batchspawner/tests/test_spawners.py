"""Test BatchSpawner and subclasses"""

import re
from unittest import mock
from .. import BatchSpawnerRegexStates
from traitlets import Unicode
import time
import pytest
from jupyterhub import orm, version_info
from tornado import gen

try:
    from jupyterhub.objects import Hub
    from jupyterhub.user import User
except:
    pass

testhost = "userhost123"
testjob  = "12345"

class BatchDummy(BatchSpawnerRegexStates):
    exec_prefix = ''
    batch_submit_cmd = Unicode('cat > /dev/null; echo '+testjob)
    batch_query_cmd = Unicode('echo RUN '+testhost)
    batch_cancel_cmd = Unicode('echo STOP')
    batch_script = Unicode('{cmd}')
    state_pending_re = Unicode('PEND')
    state_running_re = Unicode('RUN')
    state_exechost_re = Unicode('RUN (.*)$')

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
    kwargs.setdefault('hub', hub)
    kwargs.setdefault('user', user)
    kwargs.setdefault('INTERRUPT_TIMEOUT', 1)
    kwargs.setdefault('TERM_TIMEOUT', 1)
    kwargs.setdefault('KILL_TIMEOUT', 1)
    kwargs.setdefault('poll_interval', 1)
    if version_info < (0,8):
        return spawner_class(db=db, **kwargs)
    else:
        print("JupyterHub >=0.8 detected, using new spawner creation")
        return user._new_spawner('', spawner_class=spawner_class, **kwargs)

def test_stress_submit(db, io_loop):
    for i in range(200):
        time.sleep(0.01)
        test_spawner_start_stop_poll(db, io_loop)

def check_ip(spawner, value):
    if version_info < (0,7):
        assert spawner.user.server.ip == value
    else:
        assert spawner.current_ip == value

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
    with pytest.raises(AssertionError) as e_info:
        io_loop.run_sync(spawner.start, timeout=30)
    assert spawner.job_id == ''
    assert spawner.job_status == ''

def test_pending_fails(db, io_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_query_cmd = 'echo xyz'
    with pytest.raises(AssertionError) as e_info:
        io_loop.run_sync(spawner.start, timeout=30)
    assert spawner.job_id == ''
    assert spawner.job_status == ''

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

