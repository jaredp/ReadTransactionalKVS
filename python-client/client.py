import requests

OutOfDateSentinal = object()

class OutOfDateException(BaseException):
    pass

class KVServerAPI(object):
    def __init__(self, host = "http://localhost:5681/"):
        self.host = host

    def get_head(self):
        return int(requests.get(self.host + "head").content)

    def do_get(self, version, key):
        r = requests.get(self.host + "read/" + str(version) + '/' + key)

        if r.status_code == 410:
            return OutOfDateSentinal

        elif r.status_code != 200:
            # we've already checked for the errors we're expecting,
            # something went very wrong
            raise Exception("KVServer failed")

        return r.content

    def submit_transaction(self, version, deps, patch):
        # TODO: typecheck
        txn = {
            'version': version,
            'deps': deps,
            'patch': patch
        }
        # print('submitting transaction', txn)

        # send the request
        r = requests.post(self.host + "write", json=txn)

        if r.status_code == 410:
            # out of date
            return False

        elif r.status_code == 200:
            return True

        else:
            # we've already checked for the errors we're expecting,
            # something went very wrong
            raise Exception("KVServer failed")


class Transaction(object):
    def __init__(self, server):
        self.server = server
        self.version = self.server.get_head()
        self.deps = set()
        self.dirty = set()
        self.dbCache = {}
        self.printCache = []

    # transaction interaction management
    def get(self, key):
        if key in self.dbCache:
            return self.dbCache[key]

        value = self.server.do_get(self.version, key)
        if value == OutOfDateSentinal:
            raise OutOfDateException()

        self.deps.add(key)
        self.dbCache[key] = value
        return value

    def set(self, key, value):
        self.dirty.add(key)
        self.dbCache[key] = value

    # pythonic operator overloading
    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    # side effect buffering
    def prnt(self, line):
        self.printCache.append(line)

    def flush_side_effects(self):
        for line in self.printCache:
            print line

    # submit the commit
    def commit(self):
        deps = list(self.deps)
        patch = {k: str(v) for k, v in self.dbCache.items() if k in self.dirty}
        if len(patch) == 0:
            # no mutations in this transaction; no need to commit
            return True
        return self.server.submit_transaction(self.version, deps, patch)


def transaction(server, fn):
    succeeded = False
    # retry the transaction until it succeeds
    while succeeded == False:
        txn = Transaction(server)
        try:
            fn(txn)
        except OutOfDateException:
            succeeded = False
        else:
            succeeded = txn.commit()
    txn.flush_side_effects()


def with_txn(server):
    def wrapper(fn):
        return transaction(server, fn)
    return wrapper


server = KVServerAPI()

@with_txn(server)
def _(txn):
    if txn['/a'] == '':
        txn['/a'] = 10
        txn.prnt(txn['/a'])

while True:
    @with_txn(server)
    def _(txn):
        txn.prnt("got " + str(txn['/b']))
        txn.set('/b', int(txn['/a']) + int(txn['/b'] or 0) + 10)


