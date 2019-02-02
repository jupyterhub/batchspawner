import json
from tornado import web
from jupyterhub.apihandlers import  APIHandler, default_handlers

class BatchSpawnerAPIHandler(APIHandler):
    @web.authenticated
    def post(self):
        """POST set user's spawner port number"""
        if hasattr(self, 'current_user'):
            # Jupyterhub compatability, (september 2018, d79a99323ef1d)
            user = self.current_user
        else:
            # Previous jupyterhub, 0.9.4 and before.
            user = self.get_current_user()
        data = self.get_json_body()
        port = int(data.get('port', 0))
        if hasattr(user.spawner, 'child_spawner'):
            # When using wrapspawner, #127 and #129
            user.spawner.child_spawner.current_port = port
        else:
            user.spawner.current_port = port
        self.finish(json.dumps({"message": "BatchSpawner port configured"}))
        self.set_status(201)

default_handlers.append((r"/api/batchspawner", BatchSpawnerAPIHandler))
