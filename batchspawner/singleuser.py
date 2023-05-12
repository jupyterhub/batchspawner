import os
import sys

from runpy import run_path
from shutil import which

from jupyterhub.utils import random_port, url_path_join
from jupyterhub.services.auth import HubAuth

from urllib.parse import urlparse, urlunparse

import requests


def main(argv=None):
    port = random_port()
    hub_auth = HubAuth()
    hub_auth.client_ca = os.environ.get("JUPYTERHUB_SSL_CLIENT_CA", "")
    hub_auth.certfile = os.environ.get("JUPYTERHUB_SSL_CERTFILE", "")
    hub_auth.keyfile = os.environ.get("JUPYTERHUB_SSL_KEYFILE", "")

    url = url_path_join(hub_auth.api_url, "batchspawner")
    headers = {"Authorization": f"token {hub_auth.api_token}"}

    # internal_ssl kwargs
    kwargs = {}
    if hub_auth.certfile and hub_auth.keyfile:
        kwargs["cert"] = (hub_auth.certfile, hub_auth.keyfile)
    if hub_auth.client_ca:
        kwargs["verify"] = hub_auth.client_ca

    r = requests.post(
        url,
        headers={"Authorization": f"token {hub_auth.api_token}"},
        json={"port": port},
        **kwargs,
    )

    # Change the port number for the environment variable JUPYTERHUB_SERVICE_URL
    url = urlparse(os.environ['JUPYTERHUB_SERVICE_URL'])
    url_netloc = url.netloc.split(':')

    if len(url_netloc) == 2:
        url_netloc[1] = str(port)
    os.environ["JUPYTERHUB_SERVICE_URL"] = urlunparse(url._replace(netloc=':'.join(url_netloc)))

    cmd_path = which(sys.argv[1])
    sys.argv = sys.argv[1:] + ["--port={}".format(port)]
    run_path(cmd_path, run_name="__main__")


if __name__ == "__main__":
    main()
