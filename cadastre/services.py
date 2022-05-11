import os
from io import StringIO
from typing import Any

import logging
import pandas as pd
from github import Github

github = Github(os.environ["GITHUB_STORAGE_TOKEN"])
repository = github.get_user().get_repo("file_storage")


def store_file(filename: str, content: str):
    try:
        res = repository.create_file(filename, "create_file via PyGithub", content)
        return res
    except Exception as e:
        logging.error(f"cannot upload file to Github")
        logging.error(e)
        return None


def read_file(filename: str):
    try:
        file = repository.get_contents(filename)
        return file.decoded_content.decode()
    except Exception as e:
        logging.error(f"cannot retrive file {filename}")
        logging.error(e)
        return None


def read_file_to_df(filename: str, sep: str):
    file_ = read_file(filename=filename)
    if file_:
        df = pd.read_csv(StringIO(file_), sep=sep)
        return df
    else:
        return None


def delete_file(filename: str):
    try:
        file = repository.get_contents(filename)
        repository.delete_file(file.path, "Delete file", sha=file.sha)
        return True
    except Exception as e:
        logging.error(f"Cannot delete file {filename} due to below error")
        logging.error(e)
        return False
