\"\"\"Relevant pytest fixtures are re-used from JupyterHub\'s test suite\"\"\"

# event_loop was removed from jupyterhub.tests.conftest in a newer version
# Use pytest_asyncio's built-in event_loop instead
from jupyterhub.tests.conftest import db  # noqa
