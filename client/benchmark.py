import concurrent.futures
import sys

# time.time returns wall time
from time import time as timer

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt

from common_client import TransactionFailureException
import kvs_client
import redis_client
import sql_client

def benchmark(client, fn, num_transactions=200, num_workers=10, *args, **kwargs):
    start = timer()
    total_latency = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(time_transaction(fn), client, *args, **kwargs) for _ in range(num_transactions)]

        latencies = [f.result() for f in concurrent.futures.as_completed(futures)]

        total_throughput = num_transactions / (timer() - start)

        return (latencies, total_throughput)

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
    mode = int(sys.argv[1])

    if mode == 1:
        """print stats"""
        drivers = [kvs_client, sql_client, redis_client]

        for driver in drivers:
            print("-------------------")
            print("Benchmarking "+driver.__name__+"...")
            print("-------------------")
            client = driver.Connection()

            latencies, total_throughput = benchmark(client, bench1, retry=True)

            average_latency = 1/np.mean(latencies)
            print("Average latency: {} seconds per transaction".format(average_latency))

            print("Throughput: {} transactions per second".format(total_throughput))

    elif mode == 2:
        """histogram ksvserver"""
        latencies, total_throughput = benchmark(
            kvs_client.Connection(),
            bench1,
            num_transactions=200,
            num_workers=100,
            retry=False)

        plt.hist(latencies)
        plt.show()

    elif mode == 3:
        """kvs time per transaction/transaction count"""
        xs = np.arange(10, 1000, 10)
        ys = [np.mean(benchmark(
            kvs_client.Connection(),
            bench1,
            num_transactions=x,
            num_workers=100,
            retry=False)[0])
        for x in xs]

        plt.plot(xs, ys)
        plt.show()

