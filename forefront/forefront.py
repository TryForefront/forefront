import numpy as np
import requests
import tempfile
import os
import random
from typing import List, Any, Optional, NoReturn, Union, Iterable
from .api import API
from .state import State
from .datasets import Datasets
import inspect
from pathlib import Path
from prettytable import PrettyTable


def predict(endpoint: str, data: Any, key: str) -> Any:
    if endpoint is None:
        raise Exception('Must include and endpoint')
    if data is None:
        raise Exception('Must include data to call endpoint with!')
    if not isinstance(data, np.ndarray):
        raise Exception('Data must be a numpy.ndarray')

    directory = tempfile.gettempdir()
    path = os.path.join(directory, f'{random.randint(10000, 100000)}.npy')
    np.save(path, data)

    files = {'model_file': open(path, 'rb')}
    res = requests.post(endpoint, files=files, headers={
        'authorization': f"Bearer {key}"
    })

    os.remove(path)

    if (res.status_code == 404):
        raise Exception('Endpoint is down!')

    if (res.status_code == 401):
        raise Exception('Your authentication is wrong!')

    if (res.status_code != 200):
        print(res.text)
        raise Exception('Something went wrong with the request!')

    try:
        return res.json()
    except Exception:
        raise Exception('Endpoint response is malformed!')


class Forefront:
    versions: List[Any]
    key: str
    project_id: str
    organization_id: str
    api: API
    state: State
    datasets: Datasets

    def __init__(self, init_token: str = ''):
        Path(os.path.join(Path.home(), '.forefront')).mkdir(
            parents=True, exist_ok=True)
        self.state = State()
        token = self.state.get_token()

        if token == '':

            if init_token != '':
                self.state.set_token(init_token)
                print('Token saved successfully')
                self.key = self.state.get_token()
                return

            input_token = input('Please input token: ')

            self.state.set_token(input_token)
            print('Token saved successfully')

        self.key = self.state.get_token()
        self.datasets = Datasets()


    @staticmethod
    def ensure_all_forefront_dirs():
        root_path = os.path.join(Path.home(), '.forefront')
        Path(root_path).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(root_path, 'data')).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(root_path, 'tar')).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(root_path, 'upload')).mkdir(parents=True, exist_ok=True)


    def init(self, project_id: Optional[str] = None, project_name: Optional[str] = None,
             project_description: Optional[str] = None, organization_id: Optional[str] = None, ) -> NoReturn:
        if not isinstance(project_id, str) and not isinstance(project_name, str):
            self.api = API(self.key, '', '')
            projects: List[Any] = self.api.get_projects()

            out = ['(0) \t Create a new project - (new) ']
            for i, project in enumerate(projects):
                out.append(
                    f"({i + 1}) \t {project['title'][:30]} - ({project['_id']})")

            print('Select a project by either entering the number or ID')
            print()
            print('\n'.join(out))
            print()
            input_data = input('Number or ID: ').lower()

            # set project id to selected project
            if input_data == "0" or input_data == 'new':
                print('Creating a project')
                name = input('Give your project a name')
                description = input(
                    '(Optional) give your project a description')
                print('Creating project...')
                try:
                    project_id = self.api.create_project(name, description)
                    print(f"Succesfully created project {project_id}")
                    self.state.set_project_id(project_id)
                except Exception as e:
                    print('Unable to create project at this time.')

            elif input_data.isdigit() and int(input_data) > 0:
                self.state.set_project_id(projects[int(input_data) - 1]['_id'])
                self.state.set_org_id(projects[int(input_data) - 1]['orgId'])
                self.project_id = self.state.get_project_id()
                self.organization_id = self.state.get_org_id()
            else:
                filtered_projects = [
                    p for p in projects if p['_id'] == input_data.strip()]
                if len(filtered_projects) == 0:
                    raise Exception(
                        "Can't find that project. Are you sure you entered it correctly?")
                self.state.set_project_id(filtered_projects[0]['_id'])
                self.state.set_org_id(filtered_projects[0]['orgId'])
            self.api = API(self.key, self.state.get_project_id())
            self.project_id = self.state.get_project_id()
            self.organization_id = self.state.get_org_id()
            self.datasets = Datasets()
            return

        if isinstance(project_id, str):
            self.project_id = project_id
            self.api = API(key=self.key, project_id=project_id)
            print('Using the project specified')

            return

        print('No project ID provided. Creating a new project...')

        if not isinstance(project_name, str):
            raise Exception('Must supply a name to create a new project!')

        if isinstance(organization_id, str):
            print('Project will be associated with organization')
            self.api = API(key=self.key, project_id='',
                           organization_id=organization_id)

            created_project_id = self.api.create_project(
                name=project_name, description=project_description)

            self.api.project_id = created_project_id

        else:

            self.api = API(key=self.key, project_id='')

            created_project_id = self.api.create_project(
                name=project_name, description=project_description)

            self.api.project_id = created_project_id

    def list_versions(self, project_id=None) -> NoReturn:
        try:
            if project_id is None:
                project_id = self.project_id

            versions = self.api.get_versions()
            t = PrettyTable(['title', 'id', 'url', 'is_custom', 'created_at'])
            for v in versions:
                if v['endpointId'] == project_id:
                    t.add_row([v['title'], v['_id'], v['endpointUrl'],
                               v['isCustom'], v['createdAt']])
            print(f"Found {len(t._rows)} versions in project {project_id}")
            print(t)
        except:
            print("Couldn't find project id")

    def list_projects(self) -> NoReturn:
        endpoints = self.api.get_projects()
        t = PrettyTable(['title', 'id',
                         'root url', 'created_at'])
        for e in endpoints:
            t.add_row([e['title'], e['_id'], e['liveUrl'],
                       e['createdAt']])
        print(t)

    def deploy(self, model: Any, name: str, description: Optional[str] = None, model_type: Optional[str] = None,
               input_data: Optional[Any] = None,
               input_shape: Optional[List[Union[int, None]]] = None) -> NoReturn:
        self.api.deploy_version(name=name, model=model,
                                model_type=model_type, description=description, input_data=input_data,
                                input_shape=input_shape)

    def handler(self, cls: Any) -> NoReturn:
        name = cls.__name__
        keys: List[str] = cls.__dict__.keys()
        methods: List[str] = [key for key in keys if '__' not in key]
        methods.append('__init__')
        out: str = "class Handler:\n\n"
        n_failed: int = 0
        for method in methods:
            try:
                out += inspect.getsource(getattr(cls, method)) + "\n"
            except:
                n_failed += 1
        save_path = os.path.join(
            Path.home(), '.forefront', f'handler-{self.project_id}.py')

        with open(save_path, 'w') as f:
            f.write(out)

        print('Successfully saved handler for your current project!')

    def test_handler(self, cls: Any) -> NoReturn:
        keys: List[str] = cls.__dict__.keys()
        methods: List[str] = [key for key in keys if '__' not in key]
        methods.append('__init__')
        out: str = "class Handler:\n\n"
        n_failed: int = 0
        for method in methods:
            try:
                out += inspect.getsource(getattr(cls, method)) + "\n"
            except:
                n_failed += 1
        save_path = os.path.join(
            Path.home(), '.forefront', f'test-handler-{self.project_id}.py')

        with open(save_path, 'w') as f:
            f.write(out)

        # TODO: send test handler to server somehow
        print('Successfully saved test handler for your current project!')

    def set_requirements(self, packages: List[str]) -> NoReturn:
        with open(os.path.join(Path.home(), '.forefront', f'requirements-{self.state.get_project_id()}.txt'), 'w') as f:
            f.write('\n'.join(packages))

        print('Set requirements for current project!')

    def upload_dataloader(self, dataloader: Iterable, name: str, description: Optional[str] = None,
                          dataset_id: Optional[str] = None, upload_batch: Optional[int] = 32) -> NoReturn:
        self.datasets.upload_dataloader(name=name, description=description, dataloader=dataloader, dataset=dataset_id,
                                        upload_batch=upload_batch)

    def get_dataloader(self, dataset_version_id: Optional[str] = None) -> Iterable:

        return self.datasets.get_dataloader(dataset_version_id)

    def list_datasets(self):
        return self.datasets.list_datasets();

    def list_dataset_versions(self, dataset: Optional[str]):
        return self.datasets.list_dataset_versions(dataset);

    def set_default_dataset(self, dataset: str):
        return self.datasets.set_default_dataset(dataset)

    def create_dataset(self, name: str, description: Optional[str] = None, organization_id: Optional[str] = None):
        self.datasets.create_dataset(name, description, organization_id)

    def get_pytorch_dataset(self, dataset_version_id: str = None, skip_download: Optional[bool] = False) -> Any:
        return self.datasets.get_pytorch_dataset(dataset_version_id, skip_download)
