"""
.. module:: configuration
   :synopsis: Configuration file parser

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""

import os
import itertools
import copy
import logging

import yaml

log = logging.getLogger(__name__)

class ConfigurationError(Exception):
    """General exception class for Duct configuration issues
    """

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

        self.known_items = {
            'sources': list,
            'outputs': list,
            'interval': (float, int,),
            'ttl': (float, int,),
            'blueprint': list,
            'ssh_key': str,
            'ssh_user': str,
            'toolbox': dict
        }

        self._parse_config()

    def _validate_type(self, item, vtype):
        if not isinstance(vtype, tuple):
            vtype = (vtype, )

        parse = False
        for vt in vtype:
            try:
                assert isinstance(self.raw_config.get(item, vt()), vtype)
                parse = True
            except AssertionError:
                pass

        if not parse:
            raise ConfigurationError(
                f"{item} must be one of type {repr(vtype)}")

    def _validate_config(self):
        for key, val in self.known_items.items():
            self._validate_type(key, val)

    def _parse_config(self):
        self._merge_includes()

        self._validate_config()

        self._build_blueprints()

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
                                    # Merge dicts
                                    for k2, v2 in val.items():
                                        self.raw_config[key][k2] = v2

                                elif both(val, self.raw_config[key], list):
                                    # Extend lists
                                    self.raw_config[key].extend(val)
                                else:
                                    # Overwrite
                                    self.raw_config[key] = val
                            else:
                                self.raw_config[key] = val
                        log.warning('Loadded additional configuration from %s',
                                    conf_file)
            else:
                log.warning('Config Error: include_path %s does not exist',
                            ipath)

    def _build_blueprints(self):
        # Turn toolboxes into a dict
        toolboxes = self.raw_config.get('toolbox', {})
        blueprints = self.raw_config.get('blueprint', [])

        if blueprints:
            # Make sure raw config stubs exist if we have any blueprints
            if 'sources' not in self.raw_config:
                self.raw_config['sources'] = []

        for blueprint in blueprints:
            tbs = blueprint['toolbox']
            # Listify it so we can inherit multiple toolboxes
            if not isinstance(toolboxes, list):
                tbs = [tbs]

            tbs = [toolboxes[tool] for tool in tbs]

            # Compute a dot product of all the config settings vectors
            inversions = []
            for key, val in blueprint.get('sets', {}).items():
                inversions.append([(key, jay) for jay in val])

            for options in itertools.product(*inversions):
                for toolbox in tbs:
                    for source in toolbox.get('sources', []):
                        # Make a copy of the source dict
                        mysource = copy.copy(source)

                        # Merge toolbox defaults
                        for key, val in toolbox.get('defaults', {}).items():
                            mysource[key] = val

                        # Merge blueprint defaults
                        for key, val in blueprint.get('defaults', {}).items():
                            mysource[key] = val

                        # Merge set permutation
                        for key, val in options:
                            mysource[key] = val

                        # Bolt it into our real config file
                        self.raw_config['sources'].append(mysource)

        # Free a tiny bit of memory
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
