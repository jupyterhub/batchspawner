import os
import sys

from runpy import run_path
from shutil import which

from jupyterhub.utils import random_port, url_path_join
from jupyterhub.services.auth import HubAuth

def main(argv=None):
    server_name = os.environ.get('JUPYTERHUB_SERVER_NAME', '')
    port = random_port()
    hub_auth = HubAuth()
    hub_auth.client_ca = os.environ.get('JUPYTERHUB_SSL_CLIENT_CA', '')
    hub_auth.certfile = os.environ.get('JUPYTERHUB_SSL_CERTFILE', '')
    hub_auth.keyfile = os.environ.get('JUPYTERHUB_SSL_KEYFILE', '')
    hub_auth._api_request(method='POST',
                          url=url_path_join(hub_auth.api_url, 'batchspawner'),
                          json={'server_name': server_name, 'port' : port})

    cmd_path = which(sys.argv[1])
    sys.argv = sys.argv[1:] + ['--port={}'.format(port)]
    run_path(cmd_path, run_name="__main__")

if __name__ == "__main__":
    main()
