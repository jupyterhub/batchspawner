import os
import sys
import json
import asyncio

from runpy import run_path
from shutil import which

import jupyterhub
from jupyterhub.utils import random_port, url_path_join
from jupyterhub.services.auth import HubAuth

JH_MAJOR_VERSION, _, _ = jupyterhub.__version__.split('.')


def main(argv=None):
    port = random_port()
    hub_auth = HubAuth()
    hub_auth.client_ca = os.environ.get("JUPYTERHUB_SSL_CLIENT_CA", "")
    hub_auth.certfile = os.environ.get("JUPYTERHUB_SSL_CERTFILE", "")
    hub_auth.keyfile = os.environ.get("JUPYTERHUB_SSL_KEYFILE", "")
    if int(JH_MAJOR_VERSION) < 3:
        hub_auth._api_request(
            method="POST",
            url=url_path_join(hub_auth.api_url, "batchspawner"),
            json={"port": port},
        )
    else:
        # Starting from JupyterHub 3.0.0, the method _api_request is
        # asynchronous. Consequently the request is made using ASyncHTTPClient
        # and hence json argument is no longer valid. We need to change it to
        # body and encode it.
        asyncio.run(
            hub_auth._api_request(
                method="POST",
                url=url_path_join(hub_auth.api_url, "batchspawner"),
                body=json.dumps({"port": port}),
            )
        )

    cmd_path = which(sys.argv[1])
    sys.argv = sys.argv[1:] + ["--port={}".format(port)]
    run_path(cmd_path, run_name="__main__")


if __name__ == "__main__":
    main()
