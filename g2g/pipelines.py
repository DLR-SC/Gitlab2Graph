"""
Copyright (c) 2019 German Aerospace Center (DLR). All rights reserved.
SPDX-License-Identifier: MIT

Pipeline definitions

.. codeauthor:: Martin Stoffers <martin.stoffers@dlr.de>
"""
import logging
import re
from gitlab import Gitlab
from py2neo import Graph
from py2neo.ogm import NodeMatcher
from g2g import models
import uuid

log = logging.getLogger(__name__)


PIPELINES = (
    'UserPipeline',
    'LabelPipeline',
    'MilestonePipeline',
    'IssuePipeline',
    'MergeRequestPipeline',
    'CommitPipeline'
)


class PipelineException(Exception):
    pass


class Pipeline:
    """
    The Pipeline to define the processing of the data
    """

    def __init__(self, config, *args, **kwargs):
        self._name = type(self).__name__
        self._config = config
        self.gitlab_api = None
        self.graph = None
        self.matcher = None
        self.project_id = None
        self.project = None
        self._ini_gitlab_api()
        self._init_neo4j()
        self._init_project()
        self._add_constraints()

    def _ini_gitlab_api(self):
        self.gitlab_api = Gitlab(
                url='https://gitlab-ee.sc.dlr.de',
                private_token=self._config['GITLAB']['token'])

    def _init_neo4j(self):
        hostname = self._config['NEO4J']['hostname']
        protocol = self._config['NEO4J']['protocol']
        port = self._config['NEO4J']['port']
        db = self._config['NEO4J']['db']
        user = self._config['NEO4J']['user']
        password = self._config['NEO4J']['password']
        self.graph = Graph(f"{protocol}://{hostname}:{port}/{db}",
                           user=user,
                           password=password)
        self.matcher = NodeMatcher(self.graph)

    def _init_project(self):
        self.project_id = self.get_config_attribute('project_id', 'PROJECT')
        self.project = self.gitlab_api.projects.get(self.project_id)
        if 'namespace' in self.project.attributes.keys():
            self.project.attributes["namespace"] = self.project.attributes['namespace'].get('name')
        self.project_model = models.Project.create(self.graph, self.project.attributes)

    def _add_constraints(self):
        cls = getattr(models, self.name)
        cls.set_constraints(self.graph)

    @property
    def full_name(self):
        return self._name

    @property
    def name(self):
        name, _ = self._name.split("Pipeline")
        return name

    def get_config_attribute(self, attr: str, section: str = None) -> str:
        """
        Extracts an attribute from an certain section in the configuration file. \
        The default section is the pipeline name it self.

        :param attr: Attribute to be requested
        :type attr: str
        :param section: The section with the requested attribute (default: The pipeline configuration section)
        :type section: str
        :return: attribute value
        :rtype: str
        """
        if section is None:
            section = self.full_name
        try:
            pipe_section = self._config[section]
            attr_value = pipe_section[attr]
            return attr_value
        except KeyError:
            # raise PipelineException(f"Attribute '{attr}' is not defined in {section}.")
            # raise PipelineException(f"Requested pipeline '{section}' is not configured.")
            pass

    def request_data(self, *args, **kwargs):
        """
        Abstract method to implement the request of data
        """
        raise NotImplementedError()

    def transform_data(self, *args, **kwargs):
        """
        Abstract method to transform the requested data into a another format
        """
        raise NotImplementedError()

    def commit_data(self, *args, **kwargs):
        """
        Abstract method for writing transformed data to somewhere
        """
        raise NotImplementedError()


class UserPipeline(Pipeline):
    """
    A pipeline to process user information from Gitlab
    """

    def __init__(self, config, *arg, **kwargs):
        super().__init__(config, *arg, **kwargs)
        self.users = None
        self.user_model_list = []

    def request_data(self):
        self.users = self.project.users.list(all=True)

    def transform_data(self):
        for user in self.users:
            user_model = models.User.create(self.graph, user.attributes)
            user_model.belongs_to.update(self.project_model)
            self.user_model_list.append(user_model)

    def commit_data(self):
        for user_model in self.user_model_list:
            self.graph.push(user_model)


class MilestonePipeline(Pipeline):
    """
    A pipeline to process milestone information from Gitlab
    """

    def __init__(self, config, *arg, **kwargs):
        super().__init__(config, *arg, **kwargs)
        self.milestones = None
        self.milestone_model_list = []

    def request_data(self):
        self.milestones = self.project.milestones.list(all=True)

    def transform_data(self):
        for milestone in self.milestones:
            milestone_model = models.Milestone.create(self.graph, milestone.attributes)
            # milestone_model.belongs_to.update(self.project_model)
            self.milestone_model_list.append(milestone_model)

    def commit_data(self):
        for milestone_model in self.milestone_model_list:
            self.graph.push(milestone_model)


class LabelPipeline(Pipeline):
    """
    A pipeline to process milestone information from Gitlab
    """

    def __init__(self, config, *arg, **kwargs):
        super().__init__(config, *arg, **kwargs)
        self.labels = None
        self.label_model_list = []

    def request_data(self):
        self.labels = self.project.labels.list(all=True)

    def transform_data(self):
        for label in self.labels:
            label_model = models.Label.create(self.graph, label.attributes)
            # label_model.belongs_to.update(self.project_model)
            self.label_model_list.append(label_model)

    def commit_data(self):
        for label_model in self.label_model_list:
            self.graph.push(label_model)


class IssuePipeline(Pipeline):
    """
    A pipeline to process Issues from Gitlab
    """

    def __init__(self, config, *arg, **kwargs):
        super().__init__(config, *arg, **kwargs)
        self.issues = None
        self.issue_model_list = []

    def request_data(self):
        self.issues = self.project.issues.list(all=True)

    def transform_data(self):
        for issue in self.issues:
            issue_model = models.Issue.create(self.graph, issue.attributes)
            # issue_model.belongs_to.update(self.project_model)

            author_id = issue.attributes.get('author')
            if author_id:
                author_id = author_id['id']
                author_dict = models.User.match(self.graph, author_id).first()
                issue_model.created_by.update(author_dict)

            for assignee in issue.attributes.get('assignees'):
                assignee_id = assignee['id']
                author_dict = models.User.match(self.graph, assignee_id).first()
                issue_model.was_assigned.update(author_dict)

            assignee = issue.attributes.get('assignee')
            if assignee:
                assignee_id = assignee['id']
                author_dict = models.User.match(self.graph, assignee_id).first()
                issue_model.is_assigned.update(author_dict)

            closer_id = issue.attributes.get('closed_by')
            if closer_id:
                closer_id = closer_id['id']
                author_dict = models.User.match(self.graph, closer_id).first()
                issue_model.closed_by.update(author_dict)

            for label_name in issue.attributes.get('labels'):
                lbl_match = models.Label.match(self.graph).where(f"_.name =~ '{label_name}'")
                if lbl_match:
                    issue_model.has_label.update(lbl_match.first())

            milestone_id = issue.attributes.get('milestone')
            if milestone_id:
                milestone_id = milestone_id['id']
                milestone_match = models.Milestone.match(self.graph, milestone_id)
                if milestone_match:
                    issue_model.has_milestone.update(milestone_match.first())

            if issue.attributes.get('has_tasks') is True:
                task_status = issue.attributes.get('task_completion_status')
                issue_model.task_count = task_status.get('task_count')
                issue_model.task_completed = task_status.get('completed_count')

            for note in issue.notes.list():
                note_model = models.Note.get_or_create(self.graph, note.id, note.attributes)
                # note_model.belongs_to.update(self.project_model)
                if note.attributes.get('author'):
                    author_dict = note.attributes.get('author')
                    author_obj = models.User.match(self.graph, author_dict['id']).first()
                    note_model.has_author.update(author_obj)
                for award_emoji in note.awardemojis.list():
                    emoji_model = models.AwardEmoji.get_or_create(self.graph, award_emoji.id, award_emoji.attributes)
                    if award_emoji.attributes.get('user'):
                        awarder_dict = award_emoji.attributes.get('user')
                        awarder_obj = models.User.match(self.graph, awarder_dict['id']).first()
                        emoji_model.was_awarded_by.update(awarder_obj)
                    self.graph.push(emoji_model)
                    note_model.was_awarded_with.update(emoji_model)
                self.graph.push(note_model)
                issue_model.has_note.update(note_model)

            for award_emoji in issue.awardemojis.list():
                emoji_model = models.AwardEmoji.get_or_create(self.graph, award_emoji.id, award_emoji.attributes)
                if award_emoji.attributes.get('user'):
                    awarder_dict = award_emoji.attributes.get('user')
                    awarder_obj = models.User.match(self.graph, awarder_dict['id']).first()
                    emoji_model.was_awarded_by.update(awarder_obj)
                self.graph.push(emoji_model)
                issue_model.was_awarded_with.update(emoji_model)
            self.issue_model_list.append(issue_model)

    def commit_data(self):
        for issue_model in self.issue_model_list:
            self.graph.push(issue_model)


class MergeRequestPipeline(Pipeline):
    """
    A pipeline to process Merge Request information from Gitlab
    """
    def __init__(self, config, *arg, **kwargs):
        super().__init__(config, *arg, **kwargs)
        self.merge_requests = None
        self.merge_request_model_list = []

    def request_data(self):
        self.merge_requests = self.project.mergerequests.list(all=True)

    def transform_data(self):
        for merge_request in self.merge_requests:
            merge_request_model = models.MergeRequest.create(self.graph, merge_request.attributes)
            # merge_request_model.belongs_to.update(self.project_model)

            author_id = merge_request.attributes.get('author')
            if author_id:
                author_id = author_id['id']
                user = models.User.get_or_create(self.graph, pk=author_id)
                merge_request_model.created_by.update(user)

            merger_id = merge_request.attributes.get('merged_by')
            if merger_id:
                merger_id = merger_id['id']
                user = models.User.get_or_create(self.graph, pk=merger_id)
                merge_request_model.merged_by.update(user)

            for assignee in merge_request.attributes.get('assignees'):
                assignee_id = assignee['id']
                user = models.User.get_or_create(self.graph, pk=assignee_id)
                merge_request_model.was_assigned.update(user)

            assignee = merge_request.attributes.get('assignee')
            if assignee:
                assignee_id = assignee['id']
                author_dict = models.User.match(self.graph, assignee_id).first()
                merge_request_model.is_assigned.update(author_dict)

            closer_id = merge_request.attributes.get('closed_by')
            if closer_id:
                closer_id = closer_id['id']
                user = models.User.get_or_create(self.graph, pk=closer_id)
                merge_request_model.closed_by.update(user)

            for label_name in merge_request.attributes.get('labels'):
                lbl = models.Label.get(self.graph, {'name': label_name})
                if lbl:
                    merge_request_model.has_label.update(lbl)

            approve_info = merge_request.approvals.get()
            for approver in approve_info.attributes.get('approved_by'):
                approver_id = approver['user']['id']
                user = models.User.get_or_create(self.graph, pk=approver_id)
                merge_request_model.approved_by.update(user)
            merge_request_model.approvals_required = approve_info.attributes.get('approvals_required')
            merge_request_model.approvals_left = approve_info.attributes.get('approvals_left')
            merge_request_model.approved = approve_info.attributes.get('approved')

            changes = merge_request.changes()
            merge_request_model.changes_count = changes['changes_count']
            merge_request_model.merge_error = changes['merge_error']
            for change in changes['changes']:
                change['id'] = str(uuid.uuid4())
                change_model = models.Change.create(self.graph, change)
                merge_request_model.has_change.add(change_model)
                self.graph.push(change_model)

            milestone_id = merge_request.attributes.get('milestone')
            if milestone_id:
                milestone_id = milestone_id['id']
                milestone = models.Milestone.get_or_create(self.graph, pk=milestone_id)
                merge_request_model.has_milestone.update(milestone)

            merge_commit_sha = merge_request.attributes.get('merge_commit_sha')
            if merge_commit_sha:
                commit = models.Commit.get_or_create(self.graph, pk=merge_commit_sha)
                merge_request_model.has_merge_commit.update(commit)

            latest_commit = merge_request.attributes.get('sha')
            if latest_commit:
                commit = models.Commit.get_or_create(self.graph, pk=latest_commit)
                merge_request_model.is_latest_commit.update(commit)

            # Get issue from branch
            m = re.match(r"^(\d+)-.+$", merge_request_model.source_branch)
            if m is not None and m.group(1):
                issue_iid = m.group(1)
                issue = models.Issue.get(self.graph, {'iid': int(issue_iid)})
                if issue:
                    merge_request_model.is_related.update(issue)

            if merge_request.attributes.get('task_completion_status'):
                task_status = merge_request.attributes.get('task_completion_status')
                merge_request_model.task_count = task_status.get('task_count')
                merge_request_model.task_completed = task_status.get('completed_count')

            for note in merge_request.notes.list():
                note_model = models.Note.get_or_create(self.graph, note.id, note.attributes)
                # note_model.belongs_to.update(self.project_model)
                if note.attributes.get('author'):
                    author_dict = note.attributes.get('author')
                    author_obj = models.User.match(self.graph, author_dict['id']).first()
                    note_model.has_author.update(author_obj)
                for award_emoji in note.awardemojis.list():
                    emoji_model = models.AwardEmoji.get_or_create(self.graph, award_emoji.id, award_emoji.attributes)
                    if award_emoji.attributes.get('user'):
                        awarder_dict = award_emoji.attributes.get('user')
                        awarder_obj = models.User.match(self.graph, awarder_dict['id']).first()
                        emoji_model.was_awarded_by.update(awarder_obj)
                    self.graph.push(emoji_model)
                    note_model.was_awarded_with.update(emoji_model)
                self.graph.push(note_model)
                merge_request_model.has_note.update(note_model)

            for award_emoji in merge_request.awardemojis.list():
                emoji_model = models.AwardEmoji.get_or_create(self.graph, award_emoji.id, award_emoji.attributes)
                if award_emoji.attributes.get('user'):
                    awarder_dict = award_emoji.attributes.get('user')
                    awarder_obj = models.User.match(self.graph, awarder_dict['id']).first()
                    emoji_model.was_awarded_by.update(awarder_obj)
                self.graph.push(emoji_model)
                merge_request_model.was_awarded_with.update(emoji_model)

            self.merge_request_model_list.append(merge_request_model)

    def commit_data(self):
        for merge_request_model in self.merge_request_model_list:
            self.graph.push(merge_request_model)


class CommitPipeline(Pipeline):
    """
    A pipeline to process commit information from Gitlab
    """

    def __init__(self, config, *arg, **kwargs):
        super().__init__(config, *arg, **kwargs)
        self.commits = None
        self.commit_model_list = []

    def _find_user(self, name=None):
        if name is None:
            return None
        node = models.User.find(self.matcher, name__contains=name)
        if not node:
            surname = name.split()[-1]
            node = models.User.find(self.matcher, name__contains=surname)
        if node:
            return models.User.wrap(node)
        return None

    def request_data(self):
        self.commits = self.project.commits.list(all=True)

    def transform_data(self):
        for commit in self.commits:
            commit_model = models.Commit.create(self.graph, commit.attributes)
            # commit_model.belongs_to.update(self.project_model)

            author = commit.attributes.get('author_name')
            author = self._find_user(author)
            if author:
                commit_model.is_author.update(author)

            committer = commit.attributes.get('committer_name')
            committer = self._find_user(committer)
            if committer:
                commit_model.is_committer.update(committer)

            for parent_commit_id in commit.attributes.get('parent_ids'):
                parent_commit = models.Commit.get_or_create(self.graph, pk=parent_commit_id)
                commit_model.has_parent.update(parent_commit)
            self.commit_model_list.append(commit_model)

    def commit_data(self):
        for commit_model in self.commit_model_list:
            self.graph.push(commit_model)
