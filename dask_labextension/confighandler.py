"""Tornado handler for dask cluster management."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import json

from tornado import web
from notebook.base.handlers import APIHandler

from .manager import manager
from .config import GLOBAL_CONFIG

class ConfigurationHandler(APIHandler):
    """
    A tornado HTTP handler for managing the configuration of the Dask Cluster
    """
    @web.authenticated
    def get(self) -> None:
        """
        Get the current global configuration.
        """
        self.finish(json.dumps(GLOBAL_CONFIG))

    @web.authenticated
    def post(self) -> None:
        """
        Update the environment with the parameters of the cluster configuration. For security, we
        should only accept the configuration keys specified by the cluster type. 
        """
        try:
            config_update = json.loads(self.request.body.decode('utf-8'))
            for cluster_type, update in config_update.items():
                if cluster_type not in GLOBAL_CONFIG:
                    raise ValueError("Invalid cluster type to configure. Only valid types are: " + str(GLOBAL_CONFIG.keys()))
                for key in GLOBAL_CONFIG[cluster_type]:
                    if key in update:
                        GLOBAL_CONFIG[cluster_type][key]["value"] = update[key]
            self.finish(json.dumps({}))
        except:
            raise ValueError("Invalid body sent")