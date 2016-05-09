import requests

class KVServerException(Exception):
    pass

class TransactionFailureException(Exception):
    pass

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

    def submit(self, txn):
        # send the request
        r = requests.post(self.host + "write", json=txn.serialize())

        if r.status_code == 410:
            # out of date
            raise TransactionFailureException()
        elif r.status_code == 200:
            return True
        else:
            # we've already checked for the errors we're expecting,
            # something went very wrong
            raise KVServerException()

class Transaction(object):
    def __init__(self, client):
        self.client = client
        self.num_attempts = 0
        self.version = self.client.get_head()
        self.deps = set()
        self.dirty = set()
        self.dbCache = {}
        self.logger = []

    def set(self, key, value):
        self.dirty.add(key)
        self.dbCache[key] = value

    def get(self, key):
        if key in self.dbCache:
            return self.dbCache[key]

        value = self.client.read(self.version, key)
        self.deps.add(key)
        self.dbCache[key] = value

        return value

    def __setitem__(self, key, value):
        self.set(key, value)

    # transaction interaction management
    def __getitem__(self, key):
        return self.get(key)

    def log(self, line):
        self.logger.append(line)

    def flush_log(self):
        for line in self.logger:
            print(line)

    # submit the commit
    def commit(self):
        self.num_attempts += 1

        if len(self.dirty) == 0:
            # no mutations in this transaction; no need to commit
            return True

        return self.client.submit(self)

    def serialize(self):
        deps = list(self.deps)
        patch = {k: str(v) for k, v in self.dbCache.items() if k in self.dirty}

        # TODO: typecheck
        return {
            'version': self.version,
            'deps': deps,
            'patch': patch
        }
