import numpy as np
import requests
from tempfile import TemporaryFile
import tempfile
import os
import random


class Forefront:
    def __init__(self, key):
        self.key = key

    def __call__(self, endpoint, data):
        if endpoint is None:
            raise Exception('Must include and endpoint')
        if data is None:
            raise Exception('Must include data to call endpoint with!')
        if not isinstance(data, np.ndarray):
            raise Exception('Data must be a numpy.ndarray')

        directory = tempfile.gettempdir()
        path = os.path.join(directory, f'{random.randint(10000, 100000)}.npy')
        np.save(path, data)


        files = {'model_file':  open(path, 'rb')}
        res = requests.post(endpoint, files=files, headers={
            'authorization': f"Bearer {self.key}"
        })

        if(res.status_code == 404):
            raise Exception('Endpoint is down!')

        if(res.status_code == 401):
            raise Exception('Your authentication is wrong!')

        if(res.status_code != 200):
            print(res.text)
            raise Exception('Something went wrong with the request!')

        try:
            return res.json()
        except Exception:
            raise Exception('Endpoint response is malformed!')
