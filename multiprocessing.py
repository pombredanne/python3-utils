"""Functions to ease parallel processing of data"""

# built-in modules
import copy
import functools
import collections.abc

# installed modules
import multiprocess


def pool_map_more_than_one_arg(method):
    """Use this decorator when using pool with a function that
    accepts more than one argument. Note that args_or_kwargs must
    consists of either an iterable or a mapping"""
    @functools.wraps(method)
    def wrapper(args_or_kwargs):
        if isinstance(args_or_kwargs, collections.abc.Mapping):
            return method(**args_or_kwargs)
        else:
            return method(*args_or_kwargs)
    return wrapper


def _group_args(worker_args):
    if isinstance(worker_args, collections.abc.Mapping):
        # turns {1:a,b,c,d, 2:f,g,h,i} into
        # [{1:a, 2:f}, {1:b, 2:g}, {1:c, 2:h}, {1:d, 2:i}]
        worker_args_split = [
            [(k, e) for e in v] for k, v in worker_args.items()
        ]
        worker_args = [
            dict(partial)
            for partial in zip(*worker_args_split)
        ]
    else:
        # turns [[1, 2, 3, 4], [a, b, c, d]] into
        # [[1, a], [2, b], [3, c], [4, d]]
        worker_args = [list(partial) for partial in zip(*worker_args)]

    return worker_args


def pool_map(
        worker, worker_args, constant_args=None,
        cpu_ratio=0.75, single_thread=False):

    worker_args = _group_args(worker_args)

    if constant_args is not None:
        try:
            [args.update(copy.deepcopy(constant_args)) for args in worker_args]
        except AttributeError:
            [args.extend(copy.deepcopy(constant_args)) for args in worker_args]

    worker = pool_map_more_than_one_arg(worker)

    if single_thread:
        resp = map(worker, worker_args)
    else:
        count_cpu_workers = int(
            (cpu_ratio * multiprocess.cpu_count())
            if cpu_ratio <= 1.0 else cpu_ratio
        )
        pool = multiprocess.Pool(count_cpu_workers)
        resp = pool.imap(worker, worker_args)
        pool.close()
        pool.join()

    return list(resp)
