
class KVServerException(Exception):
    pass

class TransactionFailureException(Exception):
    pass

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

        deps = list(self.deps)
        patch = {k: str(v) for k, v in self.dbCache.items() if k in self.dirty}

        return self.client.submit(self.version, deps, patch)

