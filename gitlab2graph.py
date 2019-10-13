#!/usr/bin/env python

"""
Copyright (c) 2019 German Aerospace Center (DLR). All rights reserved.
SPDX-License-Identifier: MIT

A Pipeline processor to extract data from Gitlab and transform it to a valid graph representation.

.. codeauthor:: Martin Stoffers <martin.stoffers@dlr.de>
"""

import logging
import os
import argparse
from g2g.helpers import get_config, process_pipelines

LOGLEVEL = os.environ.get('LOGLEVEL', 'WARNING').upper()
logging.basicConfig(level=LOGLEVEL)


log = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Executes a pipelines for one or more Gitlab projects '
                                                 'defined by one or more configuration files.')
    parser.add_argument('configuration', type=str, nargs='+', help='Configuration file(s) in INI format')
    args = parser.parse_args()
    for config_filename in args.configuration:
        log.info("Processing pipeline defined in %s", config_filename)
        cfg = get_config(config_filename)
        process_pipelines(cfg)
