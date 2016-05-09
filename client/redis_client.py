import redis
from common_client import Transaction, TransactionFailureException, KVServerException

"""
TODOS
- Don't persist redis
- Make sure to pool/reuse connections within single proc
"""

class RedisClient(object):
    def __init__(self, client):
        self.client = client
        self.pipeline = client.pipeline()

    def get_head(self):
        # redis doesn't need versions
        return None

    def read(self, version, key):
        self.pipeline.watch(key)
        value = self.pipeline.get(key)

        if value == None:
            # key does not exist
            return ""

        return value.decode('utf-8')

    def submit(self, version, deps, patch):
        # assert deps == keys watched in pipeline

        try:
            self.pipeline.multi()
            for key, value in patch.items():
                self.pipeline.set(key, str(value))
            self.pipeline.execute()
        except redis.WatchError:
            raise TransactionFailureException()

class Connection(object):
    def __init__(self):
        pass

    def get_client(self):
        return redis.StrictRedis(host='localhost', port=6379, db=0)

    def transaction(self):
        return Transaction(RedisClient(self.get_client()))


