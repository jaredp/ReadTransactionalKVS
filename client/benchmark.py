import concurrent.futures

# FIXME this is going to measure CPU time; we need a timer that will measure
# Wall time
from timeit import default_timer as timer

from common_client import TransactionFailureException
import kvs_client
import redis_client
import sql_client

def benchmark(client, fn, num_transactions=20, num_workers=500):
    start = timer()
    total_latency = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(time_transaction(fn), client) for _ in range(num_transactions)]

        for future in concurrent.futures.as_completed(futures):
            total_latency += future.result()

        average_latency = total_latency / num_transactions
        print("Average latency: {} seconds per transaction".format(average_latency))

        throughput = num_transactions / (timer() - start)
        print("Throughput: {} transactions per second".format(throughput))


# FIXME using timeit.default_timer will result in the wrong time (!!)
def time_transaction(fn):
    def wrapped(client, *args, **kwargs):
        start = timer()
        try:
            txn = client.transaction()
            fn(txn, *args, **kwargs)
            txn.commit()
        except TransactionFailureException:
            pass
        latency = timer() - start
        return latency
    return wrapped


def bench1(txn):
    a = int(txn.get('/a') or 0)
    b = int(txn.get('/b') or 0)
    txn.set('/b', a + b + 10)

if __name__ == '__main__':
    print("-------------------")
    print("Benchmarking KVS...")
    print("-------------------")
    client = kvs_client.Connection()
    benchmark(client, bench1)

    print("------------------------")
    print("Benchmarking Postgres...")
    print("------------------------")
    client = sql_client.Connection()
    benchmark(client, bench1)

    print("---------------------")
    print("Benchmarking Redis...")
    print("---------------------")
    client = redis_client.Connection()
    benchmark(client, bench1)
