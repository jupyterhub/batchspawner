"""Test BatchSpawner and subclasses"""

from unittest import mock
from .. import BatchSpawnerRegexStates
from traitlets import Unicode
import time
import sys
import pytest
from jupyterhub import orm

class BatchDummy(BatchSpawnerRegexStates):
    batch_submit_cmd = Unicode('echo 12345')
    batch_query_cmd = Unicode('echo RUN userhost123')
    batch_cancel_cmd = Unicode('echo STOP')
    batch_script = Unicode('{cmd}')
    state_pending_re = Unicode('PEND')
    state_running_re = Unicode('RUN')
    state_exechost_re = Unicode('RUN (.*)$')

_echo_sleep = """
import sys, time
print(sys.argv)
time.sleep(30)
"""

def new_spawner(db, **kwargs):
    kwargs.setdefault('cmd', [sys.executable, '-c', _echo_sleep])
    kwargs.setdefault('user', db.query(orm.User).first())
    kwargs.setdefault('hub', db.query(orm.Hub).first())
    kwargs.setdefault('INTERRUPT_TIMEOUT', 1)
    kwargs.setdefault('TERM_TIMEOUT', 1)
    kwargs.setdefault('KILL_TIMEOUT', 1)
    kwargs.setdefault('poll_interval', 1)
    return BatchDummy(db=db, **kwargs)

def test_spawner(db, io_loop):
    spawner = new_spawner(db=db)

    status = io_loop.run_sync(spawner.poll)
    assert status == 1
    assert spawner.job_id == ''

    io_loop.run_sync(spawner.start)
    assert spawner.user.server.ip == 'userhost123'
    assert spawner.job_id == '12345'
    
    time.sleep(0.2)
    
    status = io_loop.run_sync(spawner.poll)
    assert status is None
    spawner.batch_query_cmd = 'echo NOPE'
    io_loop.run_sync(spawner.stop)
    status = io_loop.run_sync(spawner.poll)
    assert status == 1
    
def test_submit_failure(db, io_loop):
    spawner = new_spawner(db=db)
    spawner.batch_submit_cmd = 'true'
    with pytest.raises(AssertionError):
        io_loop.run_sync(spawner.start)
    assert spawner.job_id == ''
    assert spawner.job_status == ''

def test_pending_fails(db, io_loop):
    spawner = new_spawner(db=db)
    spawner.batch_query_cmd = 'echo xyz'
    with pytest.raises(AssertionError):
        io_loop.run_sync(spawner.start)
    assert spawner.job_id == ''
    assert spawner.job_status == ''

