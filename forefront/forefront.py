import numpy as np
import requests
import json

class Forefront:
    def __call__(self, endpoint, data, key):
        if endpoint is None:
            raise Exception('Must include and endpoint')
        if data is None:
            raise Exception('Must include data to call endpoint with!')
        if not isinstance(data, np.ndarray):
            raise Exception('Data must be a numpy.ndarray')

        jsonified_data = json.dumps(data.tolist())

        res = requests.post(endpoint, data=jsonified_data,
                            headers={'Content-Type': 'application/json', 'authorization': f"Bearer {key}"})

        if (res.status_code == 404):
            raise Exception('Endpoint is down!')

        if (res.status_code == 401):
            raise Exception('Your authentication is wrong!')

        if (res.status_code != 200):
            print(res.text)
            raise Exception('Something went wrong with the request!')

        try:
            return res.json()
        except:
            raise Exception('Endpoint response is malformed!')
