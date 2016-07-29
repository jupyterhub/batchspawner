"""Test BatchSpawner and subclasses"""

from unittest import mock
from .. import BatchSpawnerRegexStates
from traitlets import Unicode
import time
import sys
import pytest
from jupyterhub import orm

class BatchDummy(BatchSpawnerRegexStates):
    batch_submit_cmd = Unicode('cat > /dev/null; echo 12345')
    batch_query_cmd = Unicode('echo RUN userhost123')
    batch_cancel_cmd = Unicode('echo STOP')
    batch_script = Unicode('{cmd}')
    state_pending_re = Unicode('PEND')
    state_running_re = Unicode('RUN')
    state_exechost_re = Unicode('RUN (.*)$')

def new_spawner(db, **kwargs):
    kwargs.setdefault('cmd', ['singleuser_command'])
    kwargs.setdefault('user', db.query(orm.User).first())
    kwargs.setdefault('hub', db.query(orm.Hub).first())
    kwargs.setdefault('INTERRUPT_TIMEOUT', 1)
    kwargs.setdefault('TERM_TIMEOUT', 1)
    kwargs.setdefault('KILL_TIMEOUT', 1)
    kwargs.setdefault('poll_interval', 1)
    return BatchDummy(db=db, **kwargs)

def test_stress_submit(db, io_loop):
    for i in range(200):
        time.sleep(0.01)
        test_spawner_start_stop_poll(db, io_loop)

def test_spawner_start_stop_poll(db, io_loop):
    spawner = new_spawner(db=db)

    status = io_loop.run_sync(spawner.poll, timeout=5)
    assert status == 1
    assert spawner.job_id == ''
    assert spawner.get_state() == {}

    io_loop.run_sync(spawner.start, timeout=5)
    assert spawner.user.server.ip == 'userhost123'
    assert spawner.job_id == '12345'
    
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
    assert spawner.user.server.ip == 'userhost123'
    assert spawner.job_id == '12345'

    state = spawner.get_state()
    assert state == dict(job_id='12345', job_status='RUN userhost123')
    spawner = new_spawner(db=db)
    spawner.clear_state()
    assert spawner.get_state() == {}
    spawner.load_state(state)
    assert spawner.user.server.ip == 'userhost123'
    assert spawner.job_id == '12345'

def test_submit_failure(db, io_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_submit_cmd = 'cat > /dev/null; true'
    with pytest.raises(AssertionError) as e_info:
        io_loop.run_sync(spawner.start, timeout=30)
    assert "0 = len('')" in str(e_info.value)
    assert spawner.job_id == ''
    assert spawner.job_status == ''

def test_pending_fails(db, io_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_query_cmd = 'echo xyz'
    with pytest.raises(AssertionError) as e_info:
        io_loop.run_sync(spawner.start, timeout=30)
    assert "state_ispending" in str(e_info.value)
    assert spawner.job_id == ''
    assert spawner.job_status == ''
