import os
from typing import NoReturn
from pathlib import Path


class State:

    credentials_path: str
    global_forefront_dir: str
    project_id_path: str
    org_id_path: str

    def __init__(self):
        self.global_forefront_dir = os.path.join(Path.home(), '.forefront')
        self.credentials_path = os.path.join(
            self.global_forefront_dir, 'credentials')
        self.project_id_path = os.path.join(
            self.global_forefront_dir, 'project')
        self.org_id_path = os.path.join(self.global_forefront_dir, 'org')
        self.dataset = os.path.join(self.global_forefront_dir, 'dataset')
        self.upload_status = os.path.join(
            self.global_forefront_dir, 'upload_status')

    def set_token(self, token: str) -> NoReturn:
        with open(self.credentials_path, 'w') as f:
            f.write(token.strip())

    def get_token(self) -> str:
        try:
            with open(self.credentials_path, 'r') as f:
                token = f.read().strip()

            if token is None:
                return ''
            return token
        except:
            return ''

    def set_project_id(self, project_id: str) -> NoReturn:
        with open(self.project_id_path, 'w') as f:
            f.write(project_id.strip())

    def get_project_id(self) -> str:
        try:
            with open(self.project_id_path, 'r') as f:
                project_id = f.read().strip()

            if project_id is None:
                return ''
            return project_id.strip()
        except:
            return ''

    def get_org_id(self) -> str:
        try:
            with open(self.org_id_path, 'r') as f:
                org_id = f.read().strip()

            if org_id is None:
                return ''
            return org_id.strip()
        except:
            return ''

    def set_org_id(self, org_id: str) -> NoReturn:
        with open(self.org_id_path, 'w') as f:
            f.write(org_id.strip())

    def set_default_dataset(self, dataset: str):
        with open(self.dataset_path, 'w') as f:
            f.write(dataset.strip())

    def get_default_dataset(self, dataset: str):
        with open(self.dataset_path, 'w') as f:
            f.read().strip()

    def set_data(self, path: str, data: str) -> NoReturn:
        with open(os.path.join(self.global_forefront_dir, path), 'w') as f:
            f.write(data)

    def get_data(self, path: str) -> str:
        with open(os.path.join(self.global_forefront_dir, path), 'r') as f:
            return f.read()

    def get_default_dataset(self):
        with open(self.upload_status, 'r') as f:
            f.read().strip()

    def set_default_dataset(self, status: str):
        with open(self.upload_status, 'w') as f:
            f.write(status.strip())
