"""
Copyright (c) 2019 German Aerospace Center (DLR). All rights reserved.
SPDX-License-Identifier: MIT

Helper functions

.. codeauthor:: Martin Stoffers <martin.stoffers@dlr.de>
"""
from typing import List
import logging
import os
from os import path
import configparser
from g2g import pipelines

log = logging.getLogger(__name__)


VALID_CONFIG = {
    "GITLAB": [
        "token"
    ],
    "NEO4J": [
        "hostname",
        "protocol",
        "port",
        "db",
        "user",
        "password"
    ],
    "PROJECT": [
        "project_id"
    ]
}


class ConfigurationException(Exception):
    pass


def get_config(configfile: str) -> configparser.ConfigParser:
    """
    Parses and validates the configuration file

    :return: A valid ConfigParser instance
    :rtype: configparser.ConfigParser
    """
    config_path = path.abspath(path.join(path.dirname(path.realpath(__file__)), "../configurations"))
    configfile = path.join(config_path, configfile)
    if not os.path.exists(configfile):
        raise ConfigurationException(f"Configuration {configfile} not found.")
    log.info("Parse configuration.")
    config = configparser.ConfigParser()
    config.read(configfile)
    for section in VALID_CONFIG.keys():
        if section not in config.sections():
            raise ConfigurationException(f"Section {section} is missing")
        for param in VALID_CONFIG[section]:
            if param not in config[section]:
                raise ConfigurationException(f"Parameter {param} in section {section} is missing")
    log.info("Configuration is valid.")
    return config


def get_pipelines(config: configparser.ConfigParser) -> List[pipelines.Pipeline]:
    """
    Generates Pipelines from configuration file

    :param config: A valid ConfigParser instance
    :type config: configparser.ConfigParser
    :return: Ordered list Pipeline class instances that shall be executed
    :rtype: List[pipelines.Pipeline]
    """
    def get_pipe_instance(cls_name: str, cfg: configparser.ConfigParser) -> pipelines.Pipeline:
        cls = getattr(pipelines, cls_name)
        return cls(config=cfg)

    pipes_inst = [get_pipe_instance(pipe, config) for pipe in pipelines.PIPELINES]
    return pipes_inst


def process_pipelines(cfg: configparser.ConfigParser):
    """
    :param cfg: A valid ConfigParser instance
    :type cfg: configparser.Configparser
    """
    pipes = get_pipelines(cfg)

    # Requests data serial
    for pipe in pipes:
        log.info("Request data: %s", pipe.full_name)
        pipe.request_data()

    # Transforms and commits data
    # The order of the pipes array is defined by the order of the Pipelines in the ini file
    # Top to down
    for pipe in pipes:
        log.info("Transform data: %s", pipe.full_name)
        pipe.transform_data()
        log.info("Commit data: %s", pipe.full_name)
        pipe.commit_data()
