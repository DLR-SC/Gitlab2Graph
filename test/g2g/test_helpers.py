"""
Copyright (c) 2019 German Aerospace Center (DLR). All rights reserved.
SPDX-License-Identifier: MIT

Tests for helper function

.. codeauthor:: Martin Stoffers <martin.stoffers@dlr.de>
"""
import os
from unittest import mock
import pytest
from g2g.helpers import (VALID_CONFIG, ConfigurationException, get_config, get_pipelines)
from g2g.pipelines import Pipeline, PIPELINES


def test_get_config_success():
    test_config = get_config("project_name.ini.example")
    assert test_config["GITLAB"]["token"] == "f00b4r"


def test_get_config_file_not_found():
    test_path = os.path.abspath(os.path.join(os.getcwd(), "../configurations/"))
    with pytest.raises(ConfigurationException) as e_info:
        get_config("test_project.ini")
    assert e_info.match(f"Configuration {test_path}/test_project.ini not found.")


def test_get_config_missing_required_section():
    VALID_CONFIG["TEST_SECTION"] = []
    with pytest.raises(ConfigurationException) as e_info:
        get_config("project_name.ini.example")
    assert e_info.match("Section TEST_SECTION is missing")
    del VALID_CONFIG["TEST_SECTION"]


def test_get_config_missing_required_parameter():
    VALID_CONFIG["GITLAB"].append("TEST_PARAM")
    with pytest.raises(ConfigurationException) as e_info:
        get_config("project_name.ini.example")
    assert e_info.match("Parameter TEST_PARAM in section GITLAB is missing")
    VALID_CONFIG["GITLAB"].remove("TEST_PARAM")


@mock.patch("g2g.pipelines.Gitlab")
def test_get_pipe_instance_success(gitlab_api_mock):
    test_config = get_config("project_name.ini.example")
    cls_instances = get_pipelines(test_config)
    assert len(cls_instances) == len(PIPELINES)
    assert all([True for cls in cls_instances if isinstance(cls, Pipeline)])
