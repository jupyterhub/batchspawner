import os
import sys

from runpy import run_path
from shutil import which
from urllib.parse import urlparse

import requests
from jupyterhub.services.auth import HubAuth
from jupyterhub.utils import random_port, url_path_join


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

    requests.post(
        url,
        headers=headers,
        json={"port": port},
        **kwargs,
    )

    # Read the env var JUPYTERHUB_SERVICE_URL and replace port in the URL
    # with free port that we found here
    url = urlparse(os.environ.get("JUPYTERHUB_SERVICE_URL", ""))
    # Updated URL. We are effectively passing the port arg via env var
    if url.hostname:
        os.environ[
            "JUPYTERHUB_SERVICE_URL"
        ] = f"{url.scheme}://{url.hostname}:{port}{url.path}"

    cmd_path = which(sys.argv[1])
    sys.argv = sys.argv[1:]
    run_path(cmd_path, run_name="__main__")


if __name__ == "__main__":
    main()
