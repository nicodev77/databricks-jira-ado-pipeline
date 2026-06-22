import os
import sys

# Add working directory to path so we can import our modules two parent folders from here
file_path = os.getcwd()
repo_path = os.path.abspath(os.path.join(file_path, "../../"))
if repo_path not in sys.path:
    sys.path.append(repo_path)  # Workspace

SOLUTION_NAME = "client_dashboard"

ADO_STATUS_MAPPING = {
    # Not started
    "new (backlog-to do)": "unstarted",
    "ready for execution (approved to start)": "unstarted",

    # Active work
    "active (in-progress)": "in_progress",

    # Blocked
    "blocked (on hold)": "blocked",

    # Internal merge
    "dev complete": "merged_internally",

    # Client side
    "merged to client (resolved)": "merged_to_client",

    # Testing
    "resolved (user testing)": "user_testing",

    # Finished
    "closed (done)": "done",
}

JIRA_STATUS_MAPPING = {
    # Not started
    "backlog": "unstarted",
    "in evaluation": "unstarted",
    "approved to start": "unstarted",

    # Active work
    "in progress": "in_progress",

    # Blocked
    "blocked": "blocked",
    "on hold": "blocked",

    # Testing
    "deploy": "user_testing",
    "functional testing": "user_testing",
    "user testing": "user_testing",

    # Finished
    "done": "done",

    # Removed
    "rejected": "removed",
}
