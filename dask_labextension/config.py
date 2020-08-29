from __future__ import print_function, division, absolute_import

import os

import dask
import yaml
import subprocess
import json


GLOBAL_CONFIG = dict()


fn = os.path.join(os.path.dirname(__file__), "labextension.yaml")
dask.config.ensure_file(source=fn)

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update_defaults(defaults)


def get_parameters(cluster: str):
    return {key: entry["value"] for key, entry in GLOBAL_CONFIG[cluster].items()}


def get_environment_options(env: str):
    params = env.split(":", 1)
    result = subprocess.run("conda env list".split(), capture_output=True)
    lines = [line.split() for line in result.stdout.decode('utf-8').split('\n') if not line.startswith("#")]
    environments = {line[0]: line[-1] for line in lines if line}

    if len(params) == 2:
        _, package = params
        valid_environments = {}
        for environment in environments:
            contains_result = subprocess.run(
                ["conda", "list", "-n", environment, package, "--json"], capture_output=True)
            if len(json.loads(contains_result.stdout.decode('utf-8'))) > 0:
                valid_environments[environment] = os.path.join(environments[environment], 'bin/' + package)
        return valid_environments
    else:
        return environments


for config_id, entry in defaults["labextension"]["factories"].items():
    GLOBAL_CONFIG[config_id] = dict()
    for parameter, spec in entry["parameters"].items():
        GLOBAL_CONFIG[config_id][parameter] = spec
        if "type" not in GLOBAL_CONFIG[config_id][parameter]:
            if type(spec["value"]) is int:
                GLOBAL_CONFIG[config_id][parameter]["type"] = "int"
            elif type(spec["value"]) is str:
                GLOBAL_CONFIG[config_id][parameter]["type"] = "str"
            else:
                raise ValueError("Invalid type detected")
        if GLOBAL_CONFIG[config_id][parameter]["type"].startswith("env"):
            GLOBAL_CONFIG[config_id][parameter]["options"] = get_environment_options(
                GLOBAL_CONFIG[config_id][parameter]["type"])
