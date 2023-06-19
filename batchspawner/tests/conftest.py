"""Relevant pytest fixtures are re-used from JupyterHub's test suite"""

# We use "db" directly, but we also need event_loop
from jupyterhub.tests.conftest import db, event_loop  # noqa
