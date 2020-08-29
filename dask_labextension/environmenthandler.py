import json

from tornado import web
from notebook.base.handlers import APIHandler

from .manager import manager
import subprocess
import os


class EnvironmentHandler(APIHandler):
    """
    A tornado HTTP handler for getting the available Python environments
    """

    @web.authenticated
    def get(self, package: str = None):
        result = subprocess.run("conda env list".split(), capture_output=True)
        lines = [line.split() for line in result.stdout.decode('utf-8').split('\n') if not line.startswith("#")]
        environments = [{'name': line[0], 'path': line[-1]} for line in lines if line]
        if package is not None:
            valid_environments = []
            for environment in environments:
                contains_result = subprocess.run(["conda", "list", "-n", environment['name'], package, "--json"], capture_output=True)
                if len(json.loads(contains_result)) > 0:
                    environment['path'] = os.path.join(environment['path'], 'bin/' + package)
                    valid_environments.append(environment)
            self.finish(json.dumps(valid_environments))
        else:
            self.finish(json.dumps(environments))
