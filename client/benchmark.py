import concurrent.futures

# time.time returns wall time
from time import time as timer

from common_client import TransactionFailureException
import kvs_client
import redis_client
import sql_client

def benchmark(client, fn, num_transactions=20, num_workers=50, *args, **kwargs):
    start = timer()
    total_latency = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(time_transaction(fn), client, *args, **kwargs) for _ in range(num_transactions)]

        for future in concurrent.futures.as_completed(futures):
            total_latency += future.result()

        average_latency = total_latency / num_transactions
        print("Average latency: {} seconds per transaction".format(average_latency))

        throughput = num_transactions / (timer() - start)
        print("Throughput: {} transactions per second".format(throughput))

def time_transaction(fn):
    def wrapped(client, retry=False, *args, **kwargs):
        start = timer()
        success = False
        while success == False:
            try:
                txn = client.transaction()
                fn(txn, *args, **kwargs)
                txn.commit()
                success = True
            except TransactionFailureException:
                if not retry:
                    break
                else:
                    continue
        latency = timer() - start
        return latency
    return wrapped


def bench1(txn):
    a = int(txn.get('/a') or 0)
    b = int(txn.get('/b') or 0)
    txn.set('/b', a + b + 10)

if __name__ == '__main__':
    drivers = [kvs_client, sql_client, redis_client]

    for driver in drivers:
        print("-------------------")
        print("Benchmarking "+driver.__name__+"...")
        print("-------------------")
        client = driver.Connection()
        benchmark(client, bench1, retry=True)
