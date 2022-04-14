"""Relevant pytest fixtures are re-used from JupyterHub's test suite"""

# We only use "db" and "io_loop", but we also need event_loop which is used by
# io_loop to be available with jupyterhub 1+.
from jupyterhub.tests.conftest import db, io_loop

try:
    from jupyterhub.tests.conftest import event_loop
except:
    pass
