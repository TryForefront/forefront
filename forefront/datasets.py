from tqdm.notebook import tqdm
from .api import API
from .state import State
import numpy as np
import requests
from typing import Optional, Mapping, NoReturn, Any, List, Union, Tuple, Iterable, Generator
import os.path
from pathlib import Path
import tarfile
import os
import json
from prettytable import PrettyTable
import shutil


def save_tuple_of_numpy_arrays(tuple_of_arrays: Tuple[np.ndarray], index: int, skip_tar: Optional[bool] = False) -> List[str]:
    if not isinstance(tuple_of_arrays, tuple) and not isinstance(tuple_of_arrays, list):
        raise Exception('Data must be tuple of np.ndarray!')

    home_path = os.path.join(Path.home(), '.forefront', 'upload')

    for i, item in enumerate(tuple_of_arrays):
        filename = 'x{}_{}.npy'.format(i, index)
        save_path = os.path.join(home_path, filename)
        np.save(save_path, item)

    filenames = [os.path.join(home_path, 'x{}_{}.npy'.format(
        i, index)) for i in range(len(tuple_of_arrays))]

    # if not skip_tar:
    #     tar_path = os.path.join(home_path, '{}.tar.gz'.format(index))
    #     with tarfile.open(tar_path, 'w|gz') as f:
    #         for name in filenames:
    #             filename_without_folder = name.split('/')[-1]
    #             f.add(name, arcname=filename_without_folder)
    #
    #     for name in filenames:
    #         os.remove(name)
    #
    #     return [tar_path]
    # else:
    return filenames


def delete_contents_of_folder(folder: str):
    for file in os.listdir(folder):
        p = os.path.join(folder, file)
        if os.path.isfile(p):
            os.remove(p)


def get_data_from_numpy_files(folder: str) -> Tuple[np.ndarray]:
    result = tuple()
    for file in os.listdir(folder):
        if '.npy' in file:
            p = os.path.join(folder, file)

            data = np.load(p)

            result += (data,)

    return result


def decode_tar_path(path: str, index: int, skip_extraction: bool = False) -> Tuple[np.ndarray]:
    out_folder = os.path.join(Path.home(), '.forefront', 'data')
    with tarfile.open(path) as tar:
        tar.extractall(out_folder)

    if not skip_extraction:
        result = get_data_from_numpy_files(out_folder)

        return result


def group_tars(paths: List[str], out_path: str) -> str:
    home_path = os.path.join(Path.home(), '.forefront', 'upload')
    tar_path = out_path#os.path.join(home_path, out_path)
    with tarfile.open(tar_path, 'w|gz') as f:
        for name in paths:
            filename_without_folder = name.split('/')[-1]
            f.add(name, arcname=filename_without_folder)

    return tar_path


class Datasets:
    key: str
    endpoints: Mapping[str, str]
    methods: Mapping[str, str]
    base_endpoint: str
    organization_id: str
    state: State
    api: API
    project_id: str

    def __init__(self):
        self.state = State()

        self.key = self.state.get_token()
        self.project_id = self.state.get_project_id()
        self.organization_id = self.state.get_org_id()
        self.base_endpoint = 'https://live-server.forefront.link/api'

        self.default_dataset = self.state.get_default_dataset()

    def set_default_dataset(self, dataset: str):
        if dataset == self.default_dataset:
            return print('Dataset is already selected.')
        else:
            self.state.set_default_dataset(dataset)
            self.default_dataset = dataset
            data_path = os.path.join(Path.home(), '.forefront', 'data')
            delete_contents_of_folder(data_path)

    def make_endpoint(self, name: str) -> str:
        return f'{self.base_endpoint}/{self.endpoints[name]}'

    def make_upload_data_endpoint(self, dataset_id: str, dataset_version_id: str):
        return f'{self.base_endpoint}/datasets/{dataset_id}/versions/{dataset_version_id}/data'

    def upload_data(self, file_path: str, dataset: str, dataset_version: str) -> str:
        try:
            response = requests.post(self.make_upload_data_endpoint(dataset, dataset_version),
                                     headers={'Authorization': self.key},
                                     files={'file': open(file_path, 'rb')})
            url: str = response.json()['file']
            return url

        except Exception as e:
            raise e

    @staticmethod
    def reset_deta_folders() -> NoReturn:

        root_path = os.path.join(Path.home(), '.forefront')

        data_dir = os.path.join(root_path, 'data')
        tar_dir = os.path.join(root_path, 'tar')

        shutil.rmtree(data_dir)
        shutil.rmtree(tar_dir)

        Path(data_dir).mkdir(parents=True, exist_ok=True)
        Path(tar_dir).mkdir(parents=True, exist_ok=True)

    def upload(self, name, description, dataloader: Iterable[Tuple[np.ndarray]],
                          dataset: Optional[str] = None, upload_batch: Optional[int] = 32):

        self.reset_deta_folders()

        if not dataset:
            if self.default_dataset is not None:
                dataset = self.default_dataset
                print('Dataset not specified, using default dataset')
            else:
                self.list_datasets()
                inputted_dataset = input('Dataset not specified.\nPlease enter the ID of the dataset you want: ')
                self.default_dataset = inputted_dataset
                dataset = inputted_dataset

        dataset_version_url = self.base_endpoint + '/datasets/' + dataset + '/versions'
        data = {'name': name, 'description': description,
                'orgId': self.state.get_org_id()}
        response = requests.post(dataset_version_url, json=data, headers={
            'Authorization': self.key})

        dataset_version = response.json()['datasetVersionId']
        paths: List[str] = []
        for i, data in enumerate(tqdm(dataloader)):
            # first save iterable to local
            path: List[str] = save_tuple_of_numpy_arrays(data, i)
            for p in path:
                paths.append(p)
            # then upload
            if (i + 1) % upload_batch == 0:
                idx = (i + 1) // upload_batch
                single_path = os.path.join(Path.home(), '.forefront', 'upload', f'{idx}.tar.gz')
                single_path = group_tars(paths, single_path)
                url = self.upload_data(single_path, dataset, dataset_version)
                paths = []

    def get_dataloader(self, dataset_version_id: Optional[str] = None):

        if dataset_version_id is None:
            raise ValueError(
                'Must include a dataset version ID! Get yours from the dashboard.')

        def loader():

            i = 0
            has_more = True

            while has_more:

                save_path = os.path.join(
                    Path.home(), '.forefront', 'data', str(i))
                l = os.listdir(save_path) if os.path.isdir(save_path) else []
                files = [f for f in l if '.npy' in f]

                if len(files) > 0:
                    yield get_data_from_numpy_files(save_path)
                    i += 1
                    continue

                dataset_id = self.get_dataset_id_from_dataset_version_id(
                    dataset_version_id)
                base = self.make_upload_data_endpoint(
                    dataset_id, dataset_version_id)
                endpoint = f'{base}/{i}'
                res = requests.get(endpoint, headers={
                    'Authorization': self.key})

                if res.status_code != 200:
                    has_more = False
                    print('Finished getting data!')
                    break

                tar_folder = os.path.join(Path.home(), '.forefront', 'tar')

                if not os.path.isdir(tar_folder):
                    os.mkdir(tar_folder)
                s3_url = res.json()['url']
                data_res = requests.get(s3_url)

                tar_save_path = os.path.join(
                    Path.home(), '.forefront', 'tar', f'{i}.tar.gz')
                with open(tar_save_path, 'wb') as f:
                    f.write(data_res.content)

                data = decode_tar_path(tar_save_path, i)

                yield data

                i += 1

        return loader

    def create_dataset(self, name, description, orgId):
        datasets_url = self.base_endpoint + '/datasets'
        self.reset_deta_folders()
        data = {'name': name, 'description': description, 'orgId': orgId}
        response = requests.post(datasets_url, json=data, headers={
            'Authorization': self.key})
        return response.status_code

    def list_datasets(self):
        datasets_url = self.base_endpoint + '/datasets'

        res = requests.get(datasets_url, headers={
            'Authorization': self.key})
        data = res.json()
        t = PrettyTable(['id', 'name', 'created_at'])

        for d in data:
            try:
                t.add_row([d['_id'], d['name'], d['createdAt']])
            except KeyError:
                pass
        return print(t)

    def list_dataset_versions(self, dataset: Optional[str] = None):

        if not dataset:
            if self.default_dataset is not None:
                dataset = self.default_dataset
                print('Dataset not specified, using default dataset')
            else:
                print(
                    'Dataset not specified, and no  default dataset was found. Please specify a datset.')
                return

        datasets_url = self.base_endpoint + '/datasets/' + dataset + '/versions'
        res = requests.get(datasets_url, headers={
            'Authorization': self.key})
        data = res.json()

        t = PrettyTable(['id', 'datasetId', 'name',
                         'description', 'createdAt'])

        for d in data:
            t.add_row([d['_id'], d['datasetId'], d['name'],
                       d['description'], d['createdAt']])
        print(t)
        return

    def get_dataset_id_from_dataset_version_id(self, dataset_version_id: str) -> str:
        datasets_url = self.base_endpoint + '/datasets'
        response = requests.get(datasets_url, headers={
            'Authorization': self.key})

        datasets = response.json()

        try:
            for d in datasets:
                dataset_id = d['_id']

                datasets_url = self.base_endpoint + '/datasets/' + dataset_id + '/versions'
                versions_response = requests.get(datasets_url, headers={
                    'Authorization': self.key})

                versions = versions_response.json()

                for version in versions:
                    if version['_id'] == dataset_version_id:
                        return dataset_id

        except:
            pass

        return ''

    def quick_download_dataset(self, dataset_version_id: str = None):
        if dataset_version_id is None:
            raise ValueError('Must include a dataset version id! Get one from your dashboard.')

        i = 1
        pbar = tqdm()
        while True:
            dataset_id = self.get_dataset_id_from_dataset_version_id(
                dataset_version_id)
            base = self.make_upload_data_endpoint(
                dataset_id, dataset_version_id)
            endpoint = f'{base}/{i}'
            res = requests.get(endpoint, headers={
                'Authorization': self.key})

            if res.status_code != 200:
                print('Finished getting data!')
                break

            tar_folder = os.path.join(Path.home(), '.forefront', 'tar')
            if not os.path.isdir(tar_folder):
                os.mkdir(tar_folder)
            s3_url = res.json()['url']
            data_res = requests.get(s3_url)

            tar_save_path = os.path.join(
                Path.home(), '.forefront', 'tar', f'{i}.tar.gz')
            with open(tar_save_path, 'wb') as f:
                f.write(data_res.content)
            decode_tar_path(tar_save_path, i, skip_extraction=True)
            i += 1
            pbar.update(1)

        pbar.close()

    def get_pytorch_dataset(self, dataset_version_id: str = None, skip_download: Optional[bool] = False) -> Any:

        try:
            from forefront_pytorch import ForefrontDataset

            if not skip_download:
                self.quick_download_dataset(dataset_version_id)

            return ForefrontDataset

        except ImportError:
            raise ImportError('You must have the pytorch addon installed! pip3 install forefront[pytorch]')
