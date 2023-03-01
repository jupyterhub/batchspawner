import os
import sys

from runpy import run_path
from shutil import which

from jupyterhub.utils import random_port, url_path_join
from jupyterhub.services.auth import HubAuth

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

    cmd_path = which(sys.argv[1])
    sys.argv = sys.argv[1:] + ["--port={}".format(port)]
    run_path(cmd_path, run_name="__main__")


if __name__ == "__main__":
    main()
