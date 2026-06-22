import os
import sys


# Add working directory to path so we can import our modules three parent folders from here
file_path = os.getcwd()
repo_path = os.path.abspath(os.path.join(file_path, "../../"))
if repo_path not in sys.path:
    sys.path.append(repo_path)  # Workspace

SOLUTION_NAME = "azure_devops"

AZURE_DEVOPS_CLIENT_ID_KEY = "client-id-devops"
AZURE_DEVOPS_CLIENT_SECRET_KEY = "client-secret-devops"
AZURE_DEVOPS_CONNECTION = "azure_devops_stoicfinch"
AZURE_DEVOPS_HOST = "https://adb-1819581696362973.13.azuredatabricks.net/"
AZURE_DEVOPS_ORG = "StoicFinch"
AZURE_DEVOPS_PROJECTS = "ALL"
