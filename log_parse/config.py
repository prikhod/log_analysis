import os
import re

import yaml


class Clickhouse:
    def __init__(self, data: dict):
        self.host = data['host']
        self.port = data['port']
        self.user = data['user']
        self.password = data['password']
        self.database = data['database']
        self.secure = data['secure']


class Config:
    def __init__(self, data: dict):
        self.num_workers = data['num_workers']
        self.bytes_per_batch = data['bytes_per_batch']
        self.lines_per_batch = data['lines_per_batch']
        self.clickhouse = Clickhouse(data['clickhouse'])


def parse(filepath):
    pattern = re.compile('^"?\\$\\{([^}^{]+)\\}"?$')

    def _path_constructor(loader, node):
        value = node.value
        match = pattern.match(value)
        env_var = match.group().strip('"${}')
        return os.environ.get(env_var) + value[match.end():]

    yaml.add_implicit_resolver('env', pattern, None, yaml.SafeLoader)
    yaml.add_constructor('env', _path_constructor, yaml.SafeLoader)

    with open(filepath, "r") as f:
        cfg = yaml.load(f, Loader=yaml.SafeLoader)
    return Config(cfg)
