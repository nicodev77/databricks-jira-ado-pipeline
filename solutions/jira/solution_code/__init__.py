import os
import sys


file_path = os.getcwd()
repo_path = os.path.abspath(os.path.join(file_path, "../../"))
if repo_path not in sys.path:
    sys.path.append(repo_path)

SOLUTION_NAME = "jira"
