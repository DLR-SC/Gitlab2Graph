"""
Copyright (c) 2019 German Aerospace Center (DLR). All rights reserved.
SPDX-License-Identifier: MIT

Tests for pipelines

.. codeauthor:: Martin Stoffers <martin.stoffers@dlr.de>
"""
import pytest
from unittest import mock
from configparser import ConfigParser
from py2neo import Graph
from g2g.helpers import get_config
from g2g import pipelines
from g2g.pipelines import Pipeline, PIPELINES


@mock.patch("g2g.pipelines.Gitlab")
def test_default_pipeline_init_success(gitlab_api_mock):
    test_config = get_config("project_name.ini.example")
    test_pipe = Pipeline(test_config)
    assert isinstance(test_pipe.gitlab_api, mock.MagicMock)
    assert isinstance(test_pipe.graph, Graph)
    assert isinstance(test_pipe._config, ConfigParser)
    assert test_pipe._name == "Pipeline"
    with pytest.raises(NotImplementedError):
        test_pipe.request_data()
    with pytest.raises(NotImplementedError):
        test_pipe.transform_data()
    with pytest.raises(NotImplementedError):
        test_pipe.commit_data()


@mock.patch("g2g.pipelines.Gitlab")
def test_default_pipeline_get_config_attribute_sucess(gitlab_api_mock):
    test_config = get_config("project_name.ini.example")
    test_pipe = Pipeline(test_config)
    test_attr = test_pipe.get_config_attribute("test_attr", "ProjectPipeline")
    assert test_attr == "42"


test_pipes = [("UserPipeline", {"users": None, "user_model_list": []}),
              ("MilestonePipeline", {"milestones": None, "milestone_model_list": []}),
              ("LabelPipeline", {"labels": None, "label_model_list": []}),
              ("IssuePipeline", {"issues": None, "issue_model_list": []}),
              ("MergeRequestPipeline", {"merge_requests": None, "merge_request_model_list": []}),
              ("CommitPipeline", {"commits": None, "commit_model_list": []})]


@pytest.mark.parametrize("pipe_name,expected_attr", test_pipes)
@mock.patch("g2g.pipelines.Gitlab")
def test_pipelines_init_success(gitlab_api_mock, pipe_name, expected_attr):
    test_config = get_config("project_name.ini.example")
    pipe_cls = getattr(pipelines, pipe_name)
    test_pipe = pipe_cls(test_config)
    assert test_pipe._name == pipe_name
    for attr_name, attr_val in expected_attr.items():
        assert hasattr(test_pipe, attr_name)
        assert getattr(test_pipe, attr_name) == attr_val
