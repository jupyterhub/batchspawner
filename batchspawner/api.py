import json
from tornado import web
from jupyterhub.apihandlers import  APIHandler, default_handlers

class BatchSpawnerAPIHandler(APIHandler):
    @web.authenticated
    def post(self):
        """POST set user spawner data"""
        user = self.get_current_user()
        data = self.get_json_body()
        for key, value in data.items():
            if hasattr(user.spawner, key):
                setattr(user.spawner, key, value)
        self.finish(json.dumps({"message": "BatchSpawner data configured"}))
        self.set_status(201)

default_handlers.append((r"/api/batchspawner", BatchSpawnerAPIHandler))
