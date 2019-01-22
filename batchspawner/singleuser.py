from jupyterhub.singleuser import SingleUserNotebookApp
from jupyterhub.utils import random_port, url_path_join
from traitlets import default

class BatchSingleUserNotebookApp(SingleUserNotebookApp):
    @default('port')
    def _port(self):
        return random_port()

    def start(self):
        # Send Notebook app's port number to remote Spawner
        self.hub_auth._api_request(method='POST',
                                   url=url_path_join(self.hub_api_url, 'batchspawner'),
                                   json={'port' : self.port})
        super().start()

try:
    from jupyterlab.labapp import LabApp
except ImportError:
    BatchSingleUserLabApp = None
    pass
else:
    class BatchSingleUserLabApp(BatchSingleUserNotebookApp, LabApp):

        @default("default_url")
        def _default_url(self):
            """when using jupyter-labhub, jupyterlab is default ui"""
            return "/lab"

        def init_webapp(self, *args, **kwargs):
            super().init_webapp(*args, **kwargs)
            settings = self.web_app.settings
            if 'page_config_data' not in settings:
                settings['page_config_data'] = {}
            settings['page_config_data']['hub_prefix'] = self.hub_prefix
            settings['page_config_data']['hub_host'] = self.hub_host
            settings['page_config_data']['hub_user'] = self.user
            api_token = os.getenv('JUPYTERHUB_API_TOKEN')
            if not api_token:
                api_token = ''
            if not self.token:
                try:
                    self.token = api_token
                except AttributeError:
                    self.log.error("Can't set self.token")
            settings['page_config_data']['token'] = api_token

def main(argv=None):
    if isinstance(argv, list) and argv[0] == "lab" and BatchSingleUserLabApp != None:
        if len(argv) > 1:
            del argv[0]
        else:
            argv=None
        return BatchSingleUserLabApp.launch_instance(argv)
    else:
        return BatchSingleUserNotebookApp.launch_instance(argv)

if __name__ == "__main__":
    main()
