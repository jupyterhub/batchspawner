"""Relevant pytest fixtures are re-used from JupyterHub's test suite"""

try:
    from jupyterhub.tests.conftest import db, event_loop  # noqa
except ImportError:
    from jupyterhub.tests.conftest import db  # noqa
