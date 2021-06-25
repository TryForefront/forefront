import requests
from typing import Optional, Mapping, NoReturn, Any, List, Union
import tarfile
import os.path
from pathlib import Path
import pickle


def make_tarfile(output_filename: str, source_dir: str):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


class API:
    key: str
    endpoints: Mapping[str, str]
    methods: Mapping[str, str]
    base_endpoint: str
    organization_id: str
    project_id: str

    def __init__(self, key: str, project_id: str, organization_id: Optional[str] = None):
        self.key = key
        self.project_id = project_id
        self.organization_id = organization_id
        self.base_endpoint = 'https://api.app.tryforefront.com/api'


        self.endpoints = {
            'create_project': 'endpoints',
            'get_versions': 'versions',
            'deploy': 'versions',
            'upload': 'upload',
            'get_projects': 'endpoints'
        }

        self.methods = {
            'create_project': 'POST',
            'get_versions': 'GET',
            'deploy': 'POST',
            'upload': 'POST',
            'get_projects': 'GET'
        }

    def make_request(self, action: str, body=None) -> requests.Response:
        endpoint = self.make_endpoint(action)
        method = self.methods[action]
        return requests.request(method=method, url=endpoint, json=body,
                                headers={'Authorization': self.key, 'Content-Type': 'application/json'})

    def make_endpoint(self, name: str) -> str:
        return f'{self.base_endpoint}/{self.endpoints[name]}'

    def create_project(self, name: str, description: Optional[str] = None) -> str:

        body: Any = {
            'title': name,
            'description': description,
            'orgId': self.organization_id,
        }

        try:
            action = 'create_project'
            response = self.make_request(action=action, body=body)

            return response.json()['endpointId']

        except Exception as e:
            raise e

    def get_versions(self) -> List[Any]:
        try:
            action = 'get_versions'
            response = self.make_request(action=action)

            return response.json()

        except Exception as e:
            raise e

    def get_projects(self) -> List[Any]:
        try:
            action = 'get_projects'
            response = self.make_request(action=action)

            return response.json()

        except Exception as e:
            raise e

    def upload_file(self, file_path: str) -> str:

        try:

            response = requests.post(self.make_endpoint(self.endpoints['upload']), headers={'Authorization': self.key},
                                     files={'file': open(file_path, 'rb')})
            url: str = response.json()['image']
            return url

        except Exception as e:
            raise e

    def deploy_tensorflow(self, model: Any, name: str, description: Optional[str] = None):

        try:
            from forefront_tensorflow import convert_tensorflow_model_to_onnx

            path = os.path.join('.', 'model.onnx')
            convert_tensorflow_model_to_onnx(model, path=path)

            url: str = self.upload_file(path)
            body: Any = {
                'title': name,
                'description': description,
                'file': url,
                'orgId': self.organization_id,
                'endpointId': self.project_id
            }
            response = self.make_request('deploy', body=body)

            print('Deployed successfully!')
            print(f'Dashboard: https://app.tryforefront.com/endpoints/{self.project_id}')


        except ImportError:
            raise ImportError('You must install the forefront tensorflow extension! pip install forefront[tensorflow]')

        except Exception:
            raise Exception('Something went wrong! Please report on GitHub issues')

    def deploy_pytorch(self, model: Any, name: str, description: Optional[str] = None,
                       input_data: Optional[Any] = None):

        try:
            from forefront_pytorch import convert_pytorch_model_to_onnx

            if input_data is None:
                raise Exception('Must include input_data for a pytorch model!')

            path = os.path.join('.', 'model.onnx')
            convert_pytorch_model_to_onnx(model, input_data, path=path)

            url: str = self.upload_file(path)
            body: Any = {
                'title': name,
                'description': description,
                'file': url,
                'orgId': self.organization_id,
                'endpointId': self.project_id
            }
            response = self.make_request('deploy', body=body)

            print('Deployed successfully!')
            print(f'Dashboard: https://app.tryforefront.com/endpoints/{self.project_id}')


        except ImportError:
            raise ImportError('You must install the forefront pytorch extension! pip install forefront[pytorch]')

        except Exception:
            raise Exception('Something went wrong! Please report on GitHub issues')

    def deploy_sklearn(self, model: Any, name: str, description: Optional[str] = None,
                       input_shape: Optional[List[Union[int, None]]] = None):

        if input_shape is None or len(input_shape) == 0:
            raise Exception('Must include valid input shape for sklearn model!')

        try:
            from forefront_sklearn import convert_sklearn_model_to_onnx

            path = os.path.join('.', 'model.onnx')
            convert_sklearn_model_to_onnx(model, input_shape, path=path)

            url: str = self.upload_file(path)
            body: Any = {
                'title': name,
                'description': description,
                'file': url,
                'orgId': self.organization_id,
                'endpointId': self.project_id
            }
            response = self.make_request('deploy', body=body)

            print('Deployed successfully!')
            print(f'Dashboard: https://app.tryforefront.com/endpoints/{self.project_id}')


        except ImportError:
            raise ImportError('You must install the forefront sklearn extension! pip install forefront[sklearn]')

        except Exception:
            raise Exception('Something went wrong! Please report on GitHub issues')


    def deploy_string_path(self, path: str, name: str, description: Optional[str] = None):

        print('Uploading the file you specified...')
        try:
            url: str = self.upload_file(path)
            handler_path = os.path.join(Path.home(), '.forefront', f'handler-{self.project_id}.py')
            requirements_path = os.path.join(Path.home(), '.forefront', f'requirements-{self.project_id}.txt')

            if os.path.isfile(handler_path) and os.path.isfile(requirements_path):
                is_custom = True
                handler_url: str = self.upload_file(handler_path)
                requirements_url: str = self.upload_file(requirements_path)
            else:

                print('You have not specified a handler or requirements. Assuming this is a simple framework.')

                is_custom = None
                handler_url = None
                requirements_url = None

            body: Any = {
                'title': name,
                'description': description,
                'file': url,
                'orgId': self.organization_id,
                'endpointId': self.project_id,
                'handler': handler_url,
                'requirements': requirements_url,
                'isCustom': is_custom
            }
            response = self.make_request('deploy', body=body)

            print('Deployed successfully!')
            print(f'Dashboard: https://app.tryforefront.com/endpoints/{self.project_id}')


        except Exception as e:
            raise e

    def deploy_custom_model(self, model: Any, name: str, description: Optional[str] = None):
        path = '~/.forefront/model.pkl'

        print('Attempting to save as pickle')

        with open(path, 'wb') as f:
            pickle.dump(model, f)

        try:
            url: str = self.upload_file(path)
            body: Any = {
                'title': name,
                'description': description,
                'file': url,
                'orgId': self.organization_id,
                'endpointId': self.project_id
            }
            response = self.make_request('deploy', body=body)

            print('Deployed successfully!')
            print(f'Dashboard: https://app.tryforefront.com/endpoints/{self.project_id}')

        except Exception as e:
            raise e

    def deploy_version(self, name: str, model: Union[str, Any], description: Optional[str] = None,
                       model_type: Optional[str] = None, input_data: Optional[Any] = None,
                       input_shape: Optional[List[Union[int, None]]] = None):

        if isinstance(model, str):
            # passed a filepath to the model
            self.deploy_string_path(path=model, name=name, description=description)
            return

        if isinstance(model_type, str):
            # model type is specified
            if model_type == 'tensorflow' or model_type == 'keras':
                self.deploy_tensorflow(model, name, description)
                return
            elif model_type == 'pytorch' or model_type == 'torch':
                self.deploy_pytorch(model, name, description, input_data)
                return
            elif model_type == 'custom':
                self.deploy_custom_model(model, name, description)
                return
            elif model_type == 'sklearn' or model_type == 'scikit-learn':
                self.deploy_sklearn(model, name, description, input_shape)
                return
            else:
                raise Exception('Unknown model type!')

        # model type is not specified and model is not a file path
        # meaning we must infer the type of model
        model_type_str = str(type(model)).lower()

        if 'tensorflow' in model_type_str or 'tf' in model_type_str:
            self.deploy_tensorflow(model, name, description)
            return
        elif 'torch' in model_type_str:
            self.deploy_pytorch(model, name, description, input_data)
            return
        elif 'sklearn' in model_type_str:
            self.deploy_sklearn(model, name, description, input_shape)
            return
        else:
            raise Exception("Can't infer type of model! Try specifying your model type")

