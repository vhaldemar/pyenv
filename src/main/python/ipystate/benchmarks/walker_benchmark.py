import cloudpickle as pickle
import logging
import timeit
import sys

from dataclasses import dataclass
from logging import getLogger
from pympler import asizeof

from typing import Dict

from ipystate.impl.walker import Walker

DEFAULT_SIZE = 10 ** 5

@dataclass
class Metrics:
    """Class for keeping track single walker run statistics"""
    pref: str
    time_ms: float
    speed_mbs: float

    def to_dict(self):
        return {
            self.pref + ' time_ms'  : self.time_ms,
            self.pref + ' speed_mbs': self.speed_mbs,
        }


def benchmark_on_namespace(run_prefix: str, walker, ns: Dict[str, any]):
    sz = asizeof.asizeof(ns)
    try:
        times = timeit.repeat(lambda: walker.walk(ns), number=1)
        time_s = sum(times) / len(times)
    except Exception as e:
        logging.exception('An exception occurred:', e)
        raise e
    speed_mbs = (sz / 1e6) / time_s
    return Metrics(run_prefix, speed_mbs=speed_mbs, time_ms=time_s * 1000.)


def walker_benchmark_helper(walker) -> Dict[str, float]:
    arr = list(range(DEFAULT_SIZE))
    metrics = benchmark_on_namespace('int array', walker, {'arr': arr}).to_dict()
    for name, value in metrics.items():
        yield name, value

    class Point:
        def __init__(self, x, y):
            self.x = x
            self.y = y
    points = [Point(i, i + 1) for i in range(DEFAULT_SIZE)]
    metrics = benchmark_on_namespace('point array', walker, {'points': points}).to_dict()
    for name, value in metrics.items():
        yield name, value

    d = {str(i): i for i in range(DEFAULT_SIZE)}
    metrics = benchmark_on_namespace('dict', walker, {'d': d}).to_dict()
    for name, value in metrics.items():
        yield name, value

    recursive_dict = {}
    for _ in range(DEFAULT_SIZE):
        recursive_dict = {'dict': recursive_dict}
    metrics = benchmark_on_namespace('recursive dict', walker, {'d': d}).to_dict()
    for name, value in metrics.items():
        yield name, value


def walker_benchmark(walker, stdout=True):
    sys.setrecursionlimit(DEFAULT_SIZE + 5)

    res = {}
    for metric_name, metric_value in walker_benchmark_helper(walker):
        if stdout:
            print(f'{metric_name}: {metric_value:.3f}')
        res[metric_name] = metric_value
    order = sorted(res.keys(), key=lambda x: x[::-1])
    return {key: res[key] for key in order}


if __name__ == '__main__':
    class PickleWalker:
        def walk(self, ns):
            return pickle.dumps(ns)

    print("Pickle metrics:")
    print(walker_benchmark(walker=PickleWalker()))
    print("-" * 80)
    print()

    print("Walker metrics:")
    print(walker_benchmark(walker=Walker(getLogger())))
    print("-" * 80)
    print()
