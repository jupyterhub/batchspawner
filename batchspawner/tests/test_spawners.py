"""Test BatchSpawner and subclasses"""

from unittest import mock
from .. import BatchSpawnerRegexStates
from traitlets import Unicode
import time
import pytest
from jupyterhub import orm, version_info

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

def new_spawner(db, **kwargs):
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
        return BatchDummy(db=db, **kwargs)
    else:
        print("JupyterHub >=0.8 detected, using new spawner creation")
        return user._new_spawner('', spawner_class=BatchDummy, **kwargs)

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
