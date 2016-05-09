import requests
from common_client import Transaction, TransactionFailureException, KVServerException

class KVSClient(object):
    def __init__(self, host = "http://localhost:5681/"):
        self.host = host

    def get_head(self):
        return int(requests.get(self.host + "head").content)

    def read(self, version, key):
        r = requests.get(self.host + "read/" + str(version) + '/' + key)

        if r.status_code == 410:
            # out of date
            raise TransactionFailureException()
        elif r.status_code != 200:
            # we've already checked for the errors we're expecting,
            # something went very wrong
            raise KVServerException()

        return r.text

    def submit(self, version, deps, patch):
        # send the request
        r = requests.post(self.host + "write", json={
            'version': version,
            'deps': deps,
            'patch': patch
        })

        if r.status_code == 410:
            # out of date
            raise TransactionFailureException()
        elif r.status_code == 200:
            return True
        else:
            # we've already checked for the errors we're expecting,
            # something went very wrong
            raise KVServerException()

class Connection(object):
    def __init__(self):
        pass

    def transaction(self):
        return Transaction(KVSClient())



