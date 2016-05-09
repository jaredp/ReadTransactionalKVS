import concurrent.futures

# FIXME this is going to measure CPU time; we need a timer that will measure
# Wall time
from timeit import default_timer as timer

import kvs_client
import psycopg2
import redis

# FIXME using timeit.default_timer will result in the wrong time (!!)
def timed(fn):
    def wrapped(*args, **kwargs):
        start = timer()
        fn(*args, **kwargs)
        latency = timer() - start
        return latency
    return wrapped


def benchmark(client, fn, num_transactions=20, num_workers=100):
    start = timer()
    total_latency = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(timed(fn), client) for _ in range(num_transactions)]

        for future in concurrent.futures.as_completed(futures):
            total_latency += future.result()

        average_latency = total_latency / num_transactions
        print("Average latency: {} seconds per transaction".format(average_latency))

        throughput = num_transactions / (timer() - start)
        print("Throughput: {} transactions per second".format(throughput))

def spawn_kvs_transaction(client):
    try:
        txn = kvs_client.Transaction(client)
        a = int(txn.get('/a') or 0)
        b = int(txn.get('/b') or 0)
        txn.set('/b', a + b + 10)
        txn.commit()
    except kvs_client.TransactionFailureException:
        pass


def initialize_postgres(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS keys(key VARCHAR(20), value INT);")
        cur.execute("INSERT INTO keys VALUES('a',0);")
        cur.execute("INSERT INTO keys VALUES('b',0);")

def spawn_postgres_transaction(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * from keys WHERE key='a';")
        _, a = cur.fetchone()
        cur.execute("SELECT * from keys WHERE key='b';")
        _, b = cur.fetchone()
        cur.execute("UPDATE keys SET value={} WHERE key='b';".format(a + b + 10))

def spawn_redis_transaction(client):
    with client.pipeline() as pipe:
        try:
            pipe.watch('/a')
            pipe.watch('/b')
            a = int(pipe.get('/a') or 0)
            b = int(pipe.get('/b') or 0)
            pipe.multi()
            pipe.set('/b', a + b + 10)
            pipe.execute()
        except redis.WatchError:
            pass

if __name__ == '__main__':
    print("-------------------")
    print("Benchmarking KVS...")
    print("-------------------")
    client = kvs_client.KVSClient()
    benchmark(client, spawn_kvs_transaction)

    print("------------------------")
    print("Benchmarking Postgres...")
    print("------------------------")
    conn = psycopg2.connect(dbname='kvtransactional')
    initialize_postgres(conn)
    benchmark(conn, spawn_postgres_transaction)

    print("---------------------")
    print("Benchmarking Redis...")
    print("---------------------")
    client = redis.StrictRedis(host='localhost', port=6379, db=0)
    benchmark(client, spawn_redis_transaction)
