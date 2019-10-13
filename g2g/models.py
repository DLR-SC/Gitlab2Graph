"""
Copyright (c) 2019 German Aerospace Center (DLR). All rights reserved.
SPDX-License-Identifier: MIT

Object-Graph-Mapping for Gitlab API objects

.. codeauthor:: Martin Stoffers <martin.stoffers@dlr.de>
"""
import logging
from py2neo import Graph
from py2neo.ogm import GraphObject, Property, RelatedFrom, RelatedTo, RelatedObjects


log = logging.getLogger(__name__)


class NeoGraphObjectException(Exception):
    pass


class NeoGraphObject(GraphObject):
    __unique_constraints__ = []

    def __init__(self):
        super().__init__()
        self.__unique_constraints__ = NeoGraphObject.__unique_constraints__

    @classmethod
    def find(cls, matcher, **kwargs):
        """
        Matches the first label by a given keyword arguments
        """
        obj = matcher.match(**kwargs).first()
        return obj

    @classmethod
    def create(cls, graph: Graph, attributes: dict = None):
        """
        Create a label from given attributes.\
        At least the the __primarykey__ must available in the attributes dictionary.

        :param graph: The graph instance
        :type graph: Graph
        :param attributes: Attributes
        :type attributes: dict
        :return: The created label
        :rtype: NeoGraphObject
        """
        obj = cls()
        if cls.__primarykey__ not in attributes:
            raise NeoGraphObjectException(f"Primary '{obj.__primarykey__}' not in attributes")
        for attr, attr_value in attributes.items():
            if hasattr(obj, attr) and not isinstance(getattr(obj, attr), RelatedObjects):
                setattr(obj, attr, attr_value)
        graph.create(obj)
        return obj

    @classmethod
    def get(cls, graph: Graph, filters: dict):
        """
        Matches a labels from cls and reduces the result by the given filters.

        :param graph:
        :type graph:
        :param filters: A dictionary with filters as defined in the py2neo dockumentation
        :type filters: dict
        :return: The label with type of class
        :rtype: NeoGraphObject
        """
        obj = cls.match(graph)
        for attr, attr_value in filters.items():
            obj = obj.where(**{attr: attr_value})
        obj = obj.first()
        return obj

    @classmethod
    def get_or_create(cls, graph: Graph, pk=None, attributes: dict = None):
        """
        Serves as helper method to retrieve lables from the graph or create a new one if no label existed

        :param graph: Graph instance
        :type graph: Graph
        :param pk: The labels primary key
        :param attributes: Additional attributes used when a new label is created
        :type attributes: dict
        :return: The newly created label or an existing one.
        :rtype: NeoGraphObject
        """
        if pk is None:
            raise NeoGraphObjectException(f"Primary key missing")
        obj = cls.get(graph, filters={cls.__primarykey__: pk})
        if not obj:
            if attributes is not None:
                attributes = {cls.__primarykey__: pk, **attributes}
            else:
                attributes = {cls.__primarykey__: pk}
            obj = cls.create(graph, attributes)
        return obj

    @classmethod
    def set_constraints(cls, graph: Graph):
        """
        Sets all unique constraints defined in current class

        :param graph: The graph instance
        :type graph: Graph
        """
        for key in cls.__unique_constraints__:
            graph.schema.create_uniqueness_constraint(str(cls.__name__), key)


class Project(NeoGraphObject):

    __primarylabel__ = "Project"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
    ]

    id = Property("id")
    name = Property("name")
    description = Property("description")
    default_branch = Property("default_branch")
    created_at = Property("created_at")
    last_activity_at = Property("last_activity_at")
    namespace_name = Property("namespace_name")

    has_milestone = RelatedFrom("Milestone", "BELONGS_TO")
    has_issue = RelatedFrom("Issue", "BELONGS_TO")
    has_member = RelatedFrom("User", "BELONGS_TO")
    has_merge = RelatedFrom("MergeRequest", "BELONGS_TO")
    has_note = RelatedFrom("Note", "BELONGS_TO")


class User(NeoGraphObject):

    __primarylabel__ = "User"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
        "username"
    ]

    id = Property("id")
    name = Property("name")
    username = Property("username")
    state = Property("state")

    belongs_to = RelatedTo(Project)
    is_author = RelatedFrom("Issue", "CREATED_BY")
    is_merge_author = RelatedFrom("MergeRequest", "CREATED_BY")
    was_issue_assigned = RelatedFrom("Issue", "WAS_ASSIGNED_TO")
    was_merge_assigned = RelatedFrom("MergeRequest", "WAS_ASSIGNED_TO")
    is_issue_assigned = RelatedFrom("Issue", "IS_ASSIGNED")
    is_merge_assigned = RelatedFrom("MergeRequest", "IS_ASSIGNED")
    closed_issue = RelatedFrom("Issue", "CLOSED_BY")
    closed_merge = RelatedFrom("MergeRequest", "CLOSED_BY")
    merged_by = RelatedFrom("MergeRequest", "MERGED_BY")
    commit_author = RelatedFrom("ProjectCommit", "IS_AUTHOR")
    committer = RelatedFrom("ProjectCommit", "IS_COMMITTER")
    note_author = RelatedFrom("Note", "HAS_AUTHOR")
    awarded_emoji = RelatedFrom("AwardEmoji", "WAS_AWARDED_BY")
    approved_merge = RelatedFrom("MergeRequest", "APPROVED_BY")


class Label(NeoGraphObject):

    __primarylabel__ = "Label"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
    ]

    id = Property("id")
    name = Property("name")
    color = Property("color")
    description = Property("description")
    open_merge_requests_count = Property("open_merge_requests_count")
    open_issues_requests_count = Property("open_issues_requests_count")
    closed_issues_requests_count = Property("closed_issues_requests_count")
    is_project_label = Property("is_project_label")

    belongs_to = RelatedTo(Project)
    is_annotated = RelatedFrom("Issue", "HAS_LABEL")
    is_merge_annotated = RelatedFrom("MergeRequest", "HAS_LABEL")


class Milestone(NeoGraphObject):

    __primarylabel__ = "Milestone"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
        "iid"
    ]

    id = Property("id")
    iid = Property("iid")
    title = Property("title")
    description = Property("description")
    state = Property("state")
    created_at = Property("created_at")
    updated_at = Property("updated_at")
    due_date = Property("due_date")
    start_date = Property("start_date")

    belongs_to = RelatedTo(Project)
    is_annotated = RelatedFrom("Issue", "HAS_MILESTONE")
    is_merge_annotated = RelatedFrom("MergeRequest", "HAS_MILESTONE")


class Note(NeoGraphObject):

    __primarylabel__ = "Note"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
    ]

    id = Property("id")
    type = Property("type")
    body = Property("body")
    attachment = Property("attachment")
    system = Property("system")
    resolvable = Property("resolvable")
    created_at = Property("created_at")
    updated_at = Property("updated_at")

    has_author = RelatedTo(User)
    at_issue = RelatedFrom("Issue", "HAS_NOTE")
    at_merge = RelatedFrom("MergeRequest", "HAS_NOTE")
    belongs_to = RelatedTo(Project)
    was_awarded_with = RelatedTo("AwardEmoji")


class Issue(NeoGraphObject):

    __primarylabel__ = "Issue"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
        "iid"
    ]

    id = Property("id")
    iid = Property("iid")
    title = Property("title")
    description = Property("description")
    state = Property("state")
    weight = Property("weight")
    merge_requests_count = Property("merge_requests_count")
    created_at = Property("created_at")
    updated_at = Property("updated_at")
    closed_at = Property("updated_at")
    confidential = Property("confidential")
    due_date = Property("due_date")
    upvotes = Property("upvotes")
    downvotes = Property("downvotes")
    has_tasks = Property("has_tasks")
    task_status = Property("task_status")
    task_count = Property("task_count")
    task_completed = Property("task_completed")

    has_label = RelatedTo(Label)
    has_milestone = RelatedTo(Milestone)
    belongs_to = RelatedTo(Project)
    created_by = RelatedTo(User)
    was_assigned = RelatedTo(User)
    is_assigned = RelatedTo(User)
    closed_by = RelatedTo(User)
    is_merge_annotated = RelatedFrom("MergeRequest", "IS_RELATED")
    has_note = RelatedTo(Note)
    was_awarded_with = RelatedTo("AwardEmoji")


class MergeRequest(NeoGraphObject):

    __primarylabel__ = "MergeRequest"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
        "iid"
    ]

    id = Property("id")
    iid = Property("iid")
    title = Property("title")
    state = Property("state")
    description = Property("description")
    work_in_progress = Property("work_in_progress")
    merge_when_pipeline_succeeds = Property("merge_when_pipeline_succeeds")
    force_remove_source_branch = Property("force_remove_source_branch")
    should_remove_source_branch = Property("should_remove_source_branch")
    merge_status = Property("merge_status")
    sha = Property('sha')
    merge_commit_sha = Property('merge_commit_sha')
    reference = Property('reference')
    squash = Property('squash')
    approvals_before_merge = Property('approvals_before_merge')
    approvals_required = Property('approvals_required')
    approvals_left = Property('approvals_left')
    approved = Property('approved')
    created_at = Property("created_at")
    updated_at = Property("updated_at")
    merged_at = Property("merged_at")
    closed_at = Property("updated_at")
    target_branch = Property("target_branch")
    source_branch = Property("source_branch")
    user_notes_count = Property("user_notes_count")
    upvotes = Property("upvotes")
    downvotes = Property("downvotes")
    task_count = Property("task_count")
    task_completed = Property("task_completed")
    changes_count = Property("changes_count")
    merge_error = Property("merge_error")

    belongs_to = RelatedTo(Project)
    has_label = RelatedTo(Label)
    has_milestone = RelatedTo(Milestone)
    created_by = RelatedTo(User)
    is_assigned = RelatedTo(User)
    was_assigned = RelatedTo(User)
    closed_by = RelatedTo(User)
    merged_by = RelatedTo(User)
    is_related = RelatedTo(Issue)
    has_merge_commit = RelatedTo("Commit")
    is_latest_commit = RelatedTo("Commit")
    has_note = RelatedTo(Note)
    was_awarded_with = RelatedTo("AwardEmoji")
    approved_by = RelatedTo(User)
    has_change = RelatedTo("Change")


class Commit(NeoGraphObject):

    __primarylabel__ = "Commit"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
        "short_id"
    ]

    id = Property("id")
    short_id = Property("short_id")
    title = Property("title")
    message = Property("message")
    created_at = Property("created_at")
    author_name = Property("author_name")
    author_email = Property("author_email")
    authored_date = Property("authored_date")
    committer_name = Property("committer_name")
    committer_email = Property("committer_email")
    committed_date = Property("committed_date")

    belongs_to = RelatedTo(Project)
    is_author = RelatedTo(User)
    is_committer = RelatedTo(User)
    has_parent = RelatedTo("Commit")
    is_parent = RelatedFrom("Commit", "HAS_PARENT")
    is_merge_commit = RelatedFrom("MergeRequest", "HAS_MERGE_COMMIT")
    is_latest_commit = RelatedFrom("MergeRequest", "IS_LATEST_COMMIT")


class AwardEmoji(NeoGraphObject):

    __primarylabel__ = "AwardEmoji"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
    ]

    id = Property("id")
    name = Property("name")
    title = Property("title")
    message = Property("message")
    created_at = Property("created_at")
    updated_at = Property("created_at")

    # belongs_to = RelatedTo(Project)
    was_awarded_by = RelatedTo(User)
    was_awarded_to_note = RelatedFrom("Note", "WAS_AWARDED_WITH")
    was_awarded_to_issue = RelatedFrom("Issue", "WAS_AWARDED_WITH")
    was_awarded_to_merge = RelatedFrom("MergeRequest", "WAS_AWARDED_WITH")


class Change(NeoGraphObject):

    __primarylabel__ = "Change"
    __primarykey__ = "id"

    __unique_constraints__ = [
        "id",
    ]

    id = Property("id")
    old_path = Property("old_path")
    new_path = Property("new_path")
    a_mode = Property("a_mode")
    b_mode = Property("b_mode")
    new_file = Property("new_file")
    renamed_file = Property("renamed_file")
    deleted_file = Property("deleted_file")
    diff = Property("diff")
    changed_in_merge = RelatedFrom("MergeRequest", "HAS_CHANGE")
