import psycopg2
from common_client import Transaction, TransactionFailureException, KVServerException

class SQLClient(object):
    def __init__(self, connection):
        self.conn = connection
        # transaction started by default
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        self.curr = self.conn.cursor()

    def get_head(self):
        # SQL doesn't need versions
        return None

    def read(self, version, key):
        self.curr.execute("SELECT value FROM kvpairs WHERE key=%s;", (key,))
        sql_res = self.curr.fetchone()
        if sql_res == None:
            return ""
        else:
            (value,) = sql_res
        return value

    def submit(self, version, deps, patch):
        try:
            for key, value in patch.items():
                # manually do an upsert
                self.curr.execute("""SELECT value FROM kvpairs WHERE key=%s;""", (key,))
                if self.curr.fetchone() == None:
                    self.curr.execute("""INSERT INTO kvpairs VALUES (%s, %s);""", (key, value))
                else:
                    self.curr.execute("""UPDATE kvpairs SET value=%s WHERE key=%s;""", (value, key))
            self.conn.commit()
        except psycopg2.extensions.TransactionRollbackError:
            raise TransactionFailureException()


class Connection(object):
    def __init__(self):
        with self.get_connection().cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kvpairs (
                    key VARCHAR(100) PRIMARY KEY,
                    value VARCHAR(100)
                );
            """)

    def get_connection(self):
        return psycopg2.connect(dbname='kvtransactional')

    def transaction(self):
        return Transaction(SQLClient(self.get_connection()))


# def spawn_postgres_transaction(conn):
#     with conn.cursor() as cur:
#         cur.execute("SELECT * from keys WHERE key='a';")
#         _, a = cur.fetchone()
#         cur.execute("SELECT * from keys WHERE key='b';")
#         _, b = cur.fetchone()
#         cur.execute("UPDATE keys SET value={} WHERE key='b';".format(a + b + 10))
