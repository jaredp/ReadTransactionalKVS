import psycopg2
from common_client import Transaction, TransactionFailureException, KVServerException

class SQLClient(object):
    def __init__(self, client):
        self.client = client

    def get_head(self):
        # SQL doesn't need versions
        return None

    def read(self, version, key):
        # TODO
        pass

    def submit(self, version, deps, patch):
        # TODO
        pass

class Connection(object):
    def __init__(self):
        conn = psycopg2.connect(dbname='kvtransactional')
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS keys(
                    key VARCHAR(100) PRIMARY KEY,
                    value VARCHAR(100)
                );
            """)
            cur.execute("INSERT INTO keys VALUES('a',0);")
            cur.execute("INSERT INTO keys VALUES('b',0);")

    def transaction(self):
        return Transaction(SQLClient())


# def spawn_postgres_transaction(conn):
#     with conn.cursor() as cur:
#         cur.execute("SELECT * from keys WHERE key='a';")
#         _, a = cur.fetchone()
#         cur.execute("SELECT * from keys WHERE key='b';")
#         _, b = cur.fetchone()
#         cur.execute("UPDATE keys SET value={} WHERE key='b';".format(a + b + 10))
