"""
.. module:: configuration
   :synopsis: Configuration file parser

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import os
import itertools
import copy
import logging
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

log = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """General exception class for Duct configuration issues
    """


class DuctConfig(BaseModel):
    """Top-level configuration schema with centralised defaults."""
    model_config = ConfigDict(extra='allow')

    # Timing
    interval: float = 60.0
    ttl: float = 60.0
    stagger: float = 0.2

    # Debugging
    debug: int = 0

    # Sources and outputs
    sources: list = []
    outputs: Optional[list] = []

    # Global SSH defaults (can be overridden per-source)
    ssh_knownhosts_file: Optional[str] = '/var/lib/duct/known_hosts'
    ssh_keyfile: Optional[str] = None
    ssh_key: Optional[str] = None
    ssh_keypass: Optional[str] = None
    ssh_username: Optional[str] = None
    ssh_password: Optional[str] = None
    ssh_port: int = 22

    # Daemon config
    base_path: str = "/var/lib/duct"

class ConfigFile(object):
    """Duct configuration file parser and accessor
    """
    def __init__(self, path):
        if os.path.exists(path):
            with open(path, 'rt', encoding='utf-8') as conf:
                self.raw_config = yaml.load(conf, Loader=yaml.SafeLoader)

            if not self.raw_config:
                self.raw_config = {}
                log.warning("Warning: No configuration content")
        else:
            raise ConfigurationError(f"Configuration file '{path}' not found")

        self._parse_config()

    def _parse_config(self):
        self._merge_includes()
        self._build_blueprints()

        try:
            self.duct_config = DuctConfig(**self.raw_config)
        except ValidationError as e:
            raise ConfigurationError(str(e)) from e

        self.raw_config = self.duct_config.model_dump()

    def _merge_includes(self):
        def both(i1, i2, t):
            return isinstance(i1, t) and isinstance(i2, t)

        paths = self.raw_config.get('include_path', [])
        if not isinstance(paths, list):
            paths = [paths]

        paths2 = self.raw_config.get('include', [])
        if not isinstance(paths2, list):
            paths2 = [paths2]

        paths.extend(paths2)

        for ipath in paths:
            if os.path.exists(ipath):
                files = [os.path.join(ipath, fi) for fi in os.listdir(ipath)
                         if fi.endswith('.yml') or fi.endswith('.yaml')]

                for conf_file in files:
                    with open(conf_file, 'rt', encoding='utf-8') as yaml_path:
                        conf = yaml.load(yaml_path, Loader=yaml.SafeLoader)
                        for key, val in conf.items():
                            if key in self.raw_config:
                                if both(val, self.raw_config[key], dict):
                                    for k2, v2 in val.items():
                                        self.raw_config[key][k2] = v2

                                elif both(val, self.raw_config[key], list):
                                    self.raw_config[key].extend(val)
                                else:
                                    self.raw_config[key] = val
                            else:
                                self.raw_config[key] = val
                        log.warning('Loadded additional configuration from %s',
                                    conf_file)
            else:
                log.warning('Config Error: include_path %s does not exist',
                            ipath)

    def _build_blueprints(self):
        toolboxes = self.raw_config.get('toolbox', {})
        blueprints = self.raw_config.get('blueprint', [])

        if blueprints:
            if 'sources' not in self.raw_config:
                self.raw_config['sources'] = []

        for blueprint in blueprints:
            tbs = blueprint['toolbox']
            if not isinstance(toolboxes, list):
                tbs = [tbs]

            tbs = [toolboxes[tool] for tool in tbs]

            inversions = []
            for key, val in blueprint.get('sets', {}).items():
                inversions.append([(key, jay) for jay in val])

            for options in itertools.product(*inversions):
                for toolbox in tbs:
                    for source in toolbox.get('sources', []):
                        mysource = copy.copy(source)

                        for key, val in toolbox.get('defaults', {}).items():
                            mysource[key] = val

                        for key, val in blueprint.get('defaults', {}).items():
                            mysource[key] = val

                        for key, val in options:
                            mysource[key] = val

                        self.raw_config['sources'].append(mysource)

        if 'toolbox' in self.raw_config:
            del self.raw_config['toolbox']

        if 'blueprint' in self.raw_config:
            del self.raw_config['blueprint']

    def get(self, item, default=None):
        """Returns `item` from configuration if it exists, otherwise returns
           `default`
        """
        return self.raw_config.get(item, default)

    def __getitem__(self, item):
        return self.raw_config[item]

    def __getattr__(self, name):
        return self.duct_config.__getattribute__(name)
