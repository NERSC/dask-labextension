"""A Jupyter notebook server extension for managing Dask clusters."""

from notebook.utils import url_path_join

from . import config
from .clusterhandler import DaskClusterHandler
from .dashboardhandler import DaskDashboardHandler
from .environmenthandler import EnvironmentHandler
from .confighandler import ConfigurationHandler


from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


def _jupyter_server_extension_paths():
    return [{"module": "dask_labextension"}]


def load_jupyter_server_extension(nb_server_app):
    """
    Called when the extension is loaded.

    Args:
        nb_server_app (NotebookWebApplication): handle to the Notebook webserver instance.
    """
    cluster_id_regex = r"(?P<cluster_id>\w+-\w+-\w+-\w+-\w+)"
    web_app = nb_server_app.web_app
    base_url = web_app.settings["base_url"]
    get_cluster_path = url_path_join(base_url, "dask/clusters/" + cluster_id_regex)
    list_clusters_path = url_path_join(base_url, "dask/clusters/" + "?")
    get_dashboard_path = url_path_join(
        base_url, f"dask/dashboard/{cluster_id_regex}(?P<proxied_path>.+)"
    )
    get_environment_path = url_path_join(base_url, f"dash/environments")


    get_configuration_path = url_path_join(base_url, "dask/config")
    set_configuration_path = url_path_join(base_url, "dask/config")

    handlers = [
        (get_cluster_path, DaskClusterHandler),
        (list_clusters_path, DaskClusterHandler),
        (get_dashboard_path, DaskDashboardHandler),
        (get_environment_path, EnvironmentHandler),
        (get_configuration_path, ConfigurationHandler),
        (set_configuration_path, ConfigurationHandler)
    ]
    web_app.add_handlers(".*$", handlers)
