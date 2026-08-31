"""Teach core's project verbs how to handle a workflow workspace member.

Core owns ``httk project doctor|manifest|seal|unseal|verify-seal`` and delegates
a member's internals to the handler its kind registers. Registering the
``"workspace"`` kind is what makes a workflow workspace a first-class project
member those verbs seal, exclude, verify, and check.
"""

from httk.core.register.members import register_project_member_kind

register_project_member_kind("workspace", "httk.workflow.project_member:handler")
