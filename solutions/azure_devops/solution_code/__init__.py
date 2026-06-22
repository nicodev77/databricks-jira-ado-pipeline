import os
import sys


# Add working directory to path so we can import our modules three parent folders from here
file_path = os.getcwd()
repo_path = os.path.abspath(os.path.join(file_path, "../../"))
if repo_path not in sys.path:
    sys.path.append(repo_path)  # Workspace

SOLUTION_NAME = "azure_devops"


